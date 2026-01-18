from airflow.sdk import dag, task, Param
from airflow.sdk.bases.hook import BaseHook
from airflow.utils.task_group import TaskGroup
from pendulum import datetime

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from fpdf import FPDF
from keplergl import KeplerGl
import boto3

# ============================================================
# CONFIGURACIÓN Y UTILIDADES
# ============================================================
S3_BUCKET = "mobility-shared-2"

def safe_text(text: str) -> str:
    return str(text).encode("latin-1", errors="ignore").decode("latin-1")

def resolve_gold_table(tipo_zona: str) -> str:
    if tipo_zona == "municipios":
        return "gold.mobility_municipios_periodo"
    elif tipo_zona == "distritos":
        return "gold.mobility_distritos_periodo"
    else:
        raise ValueError(f"Tipo de zona no soportado: {tipo_zona}")

def get_con_and_attach():
    aws = BaseHook.get_connection("aws_s3_conn")
    pg = BaseHook.get_connection("my_postgres_conn")

    con = duckdb.connect(database=":memory:")
    con.execute("""
        INSTALL ducklake; INSTALL spatial; INSTALL postgres;
        LOAD ducklake; LOAD spatial; LOAD postgres;
    """)

    con.execute(f"""
        SET s3_region='{aws.extra_dejson.get("region", "eu-central-1")}';
        SET s3_access_key_id='{aws.login}';
        SET s3_secret_access_key='{aws.password}';
        SET s3_url_style='path';
    """)

    db_name = pg.schema or "neondb"
    con.execute(f"""
        CREATE OR REPLACE SECRET secreto_postgres (
            TYPE postgres,
            HOST '{pg.host}',
            PORT {pg.port},
            DATABASE '{db_name}',
            USER '{pg.login}',
            PASSWORD '{pg.password}'
        );

        CREATE OR REPLACE SECRET secreto_ducklake (
            TYPE ducklake,
            METADATA_PATH '',
            METADATA_PARAMETERS MAP {{'TYPE':'postgres','SECRET':'secreto_postgres'}}
        );

        ATTACH 'ducklake:secreto_ducklake'
        AS movilidad (DATA_PATH 's3://mobility-shared-2/', OVERRIDE_DATA_PATH TRUE);

        USE movilidad;
    """)
    return con

def upload_to_s3_bq(file_path, bq_folder, filename):
    aws = BaseHook.get_connection("aws_s3_conn")
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws.login,
        aws_secret_access_key=aws.password,
        region_name=aws.extra_dejson.get("region", "eu-central-1")
    )
    s3.upload_file(file_path, S3_BUCKET, f"gold/{bq_folder}/{filename}")

def df_to_xcom(df: pd.DataFrame) -> str:
    return df.to_json(orient="split")

def df_from_xcom(s: str) -> pd.DataFrame:
    return pd.read_json(s, orient="split")

# ============================================================
# DAG
# ============================================================
@dag(
    dag_id="gold_bq_nuevo",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "polygon": Param(
            "POLYGON((-0.5513 39.5784, -0.2104 39.5784, -0.2104 39.3175, -0.5513 39.3175, -0.5513 39.5784))",
            type="string",
        ),
        "start_date": Param("2023-01-01", type="string"),
        "end_date": Param("2023-01-02", type="string"),
        "tipo_zona": Param(
            "municipios",
            enum=["municipios", "distritos"],
            type="string",
        ),
    },
)
def gold_bq_dag():

    # =========================================================
    # BQ1 — CLUSTERING DE DÍAS POR PATRÓN HORARIO
    # =========================================================
    with TaskGroup("BQ1_Day_Type_Clustering"):

        # -----------------------------------------------------
        # 1. EXTRACT + DATASET POR DÍA
        # -----------------------------------------------------
        @task
        def bq1_extract_days(**context):
            p = context["params"]
            gold = resolve_gold_table(p["tipo_zona"])
            con = get_con_and_attach()

            # Dataset: un vector horario por DÍA
            df = con.execute(f"""
                SELECT
                    v.fecha,
                    CAST(v.periodo AS INTEGER) AS periodo,
                    SUM(v.total_viajes) AS total_viajes
                FROM {gold} v
                JOIN silver.lugares l ON v.id_origen = l.id
                WHERE v.fecha BETWEEN '{p["start_date"]}' AND '{p["end_date"]}'
                AND ST_Contains(
                    ST_GeomFromText('{p["polygon"]}'),
                    l.coordenadas
                )
                GROUP BY 1, 2
                ORDER BY 1, 2
            """).df()

            con.close()
            return df_to_xcom(df)

        # -----------------------------------------------------
        # 2. CLUSTERING + PDF
        # -----------------------------------------------------
        @task
        def bq1_cluster_days(df_json: str, **context):
            run_id = context["run_id"]

            df = df_from_xcom(df_json)
            if df.empty:
                return

            # -------------------------------------------------
            # Pivot: filas = días, columnas = horas (0..23)
            # -------------------------------------------------
            hours = list(range(24))
            pivot = (
                df.pivot(index="fecha", columns="periodo", values="total_viajes")
                .reindex(columns=hours, fill_value=0)
            )

            # -------------------------------------------------
            # Normalización por día (forma del perfil)
            # -------------------------------------------------
            X = pivot.div(pivot.sum(axis=1), axis=0).fillna(0)

            # -------------------------------------------------
            # KMeans (k = 3)
            # -------------------------------------------------
            kmeans = KMeans(n_clusters=3, random_state=42)
            labels = kmeans.fit_predict(X)

            pivot["cluster"] = labels

            # -------------------------------------------------
            # Perfil medio por cluster
            # -------------------------------------------------
            cluster_profiles = (
                pivot
                .groupby("cluster")
                .mean()
                .drop(columns="cluster", errors="ignore")
            )

            # -------------------------------------------------
            # GRÁFICA FINAL
            # -------------------------------------------------
            plt.figure(figsize=(10, 5))

            for c in sorted(cluster_profiles.index):
                plt.plot(
                    cluster_profiles.loc[c],
                    marker="o",
                    label=f"Cluster {c + 1}",
                    linewidth=2
                )

            plt.xlabel("Hora del día")
            plt.ylabel("Proporción de viajes")
            plt.title("Perfiles horarios de movilidad por tipo de día")
            plt.legend()
            plt.grid(alpha=0.3)

            plot_path = f"/tmp/bq1_day_clusters_{run_id}.png"
            plt.savefig(plot_path)
            plt.close()

            # -------------------------------------------------
            # PDF FINAL (ÚNICO OUTPUT)
            # -------------------------------------------------
            pdf = FPDF()
            pdf.add_page()

            pdf.set_font("Arial", "B", 16)
            pdf.cell(
                0, 15,
                safe_text("BQ1: Tipos de día según patrón horario de movilidad"),
                ln=True,
                align="C"
            )

            pdf.ln(5)
            pdf.set_font("Arial", "", 11)
            pdf.multi_cell(
                0, 7,
                safe_text(
                    "Este análisis identifica patrones diarios de movilidad "
                    "mediante clustering no supervisado (K-means, k=3), "
                    "utilizando perfiles horarios agregados de viajes. "
                    "Cada cluster representa un tipo de día con un comportamiento "
                    "de movilidad característico."
                )
            )

            pdf.ln(5)
            pdf.image(plot_path, x=10, w=190)

            pdf_path = f"/tmp/bq1_day_type_report_{run_id}.pdf"
            pdf.output(pdf_path)

            upload_to_s3_bq(pdf_path, "BQ1", "day_type_clustering.pdf")

        # -----------------------------------------------------
        # ENCADENADO
        # -----------------------------------------------------
        bq1_cluster_days(bq1_extract_days())

    # =========================================================
    # BQ2 — GRAVITY MISMATCH
    # =========================================================
    with TaskGroup("BQ2_Gravity_Mismatch"):

        # -----------------------------------------------------
        # 1. EXTRACT
        # -----------------------------------------------------
        @task
        def bq2_extract(**context):
            p = context["params"]
            gold = resolve_gold_table(p["tipo_zona"])
            con = get_con_and_attach()

            df = con.execute(f"""
                WITH base AS (
                    SELECT
                        mo.nombre AS origen,
                        md.nombre AS destino,
                        mo.latitude  AS o_lat,
                        mo.longitude AS o_lon,
                        md.latitude  AS d_lat,
                        md.longitude AS d_lon,
                        SUM(v.total_viajes) AS viajes_reales,

                        -- Distancia media ponderada
                        SUM(v.total_viajes * v.avg_viajes_metros)
                            / NULLIF(SUM(v.total_viajes), 0) AS dist_media,

                        -- Variables demográficas
                        CAST(MAX(po.poblacion) AS DOUBLE) AS pop_origen,
                        CAST(MAX(pd.poblacion) AS DOUBLE) AS pop_destino,
                        CAST(MAX(r.renta) AS DOUBLE)      AS renta_destino

                    FROM {gold} v
                    JOIN silver.lugares mo   ON v.id_origen  = mo.id
                    JOIN silver.lugares md   ON v.id_destino = md.id
                    JOIN silver.poblacion po ON v.id_origen  = po.id
                    JOIN silver.poblacion pd ON v.id_destino = pd.id
                    JOIN silver.renta r      ON v.id_destino = r.id

                    WHERE v.fecha BETWEEN '{p["start_date"]}' AND '{p["end_date"]}'
                    AND ST_Contains(ST_GeomFromText('{p["polygon"]}'), mo.coordenadas)

                    GROUP BY
                        mo.nombre, md.nombre,
                        mo.latitude, mo.longitude,
                        md.latitude, md.longitude
                )
                SELECT
                    *,
                    -- Potencial gravitacional clásico
                    (pop_origen * renta_destino)
                        / NULLIF(POWER(dist_media, 2), 0) AS potencial
                FROM base
            """).df()

            con.close()
            return df_to_xcom(df)

        # -----------------------------------------------------
        # 2. ASSETS (MAPA + GRÁFICO + RANKING)
        # -----------------------------------------------------
        @task
        def bq2_assets(df_json: str, **context):
            run_id = context["run_id"]

            df = df_from_xcom(df_json)

            # --------------------------------------------------
            # 1. Normalización global (k)
            # --------------------------------------------------
            k = df.viajes_reales.sum() / df.potencial.sum()

            # --------------------------------------------------
            # 2. Mismatch clásico
            # --------------------------------------------------
            df["mismatch"] = df.viajes_reales / (df.potencial * k)

            # --------------------------------------------------
            # 3. Ponderación demográfica (OPCIÓN B)
            # --------------------------------------------------
            df["pop_weight"] = (df["pop_origen"] * df["pop_destino"]) ** 0.5

            df["weighted_mismatch"] = df["mismatch"] * df["pop_weight"]

            # --------------------------------------------------
            # MAPA (menor weighted_mismatch)
            # --------------------------------------------------
            df_map = df.sort_values("weighted_mismatch").head(50)

            map_path = f"/tmp/bq2_map_{run_id}.html"
            KeplerGl(
                data={"Desconexiones_Gravity": df_map}
            ).save_to_html(file_name=map_path)

            # --------------------------------------------------
            # GRÁFICO (mayor weighted_mismatch)
            # --------------------------------------------------
            df_plot = df.sort_values("weighted_mismatch", ascending=False).head(10)

            plt.figure(figsize=(10, 6))
            sns.barplot(
                data=df_plot,
                x="weighted_mismatch",
                y="destino",
                hue="origen",
                palette="magma"
            )
            plt.tight_layout()

            plot_path = f"/tmp/bq2_plot_{run_id}.png"
            plt.savefig(plot_path)
            plt.close()

            # --------------------------------------------------
            # RUTAS CRÍTICAS (TOP 15)
            # --------------------------------------------------
            df_critical = df.sort_values(
                "weighted_mismatch", ascending=False
            ).head(15)

            return {
                "map_path": map_path,
                "plot_path": plot_path,
                "df_critical": df_to_xcom(df_critical),
            }

                # -----------------------------------------------------
        # 3. PDF + UPLOAD
        # -----------------------------------------------------
        @task
        def bq2_pdf_upload(payload: dict, **context):
            run_id = context["run_id"]

            df_critical = df_from_xcom(payload["df_critical"])

            pdf = FPDF()
            pdf.add_page()

            pdf.set_font("Arial", "B", 16)
            pdf.cell(
                0, 15,
                safe_text("BQ2: Reporte de Desconexión (Gravity Mismatch)"),
                ln=True, align="C"
            )

            # Gráfico
            pdf.image(payload["plot_path"], x=10, w=180)

            # Listado de rutas críticas (IGUAL QUE EL ORIGINAL)
            pdf.ln(10)
            pdf.set_font("Arial", "B", 12)
            pdf.cell(0, 10, safe_text("Listado de Rutas Críticas:"), ln=True)

            pdf.set_font("Arial", "", 10)
            for _, r in df_critical.iterrows():
                txt = (
                        f"{r.origen} -> {r.destino} | "
                        f"Weighted mismatch: {r.weighted_mismatch:.4f}"
                    )

                pdf.cell(0, 7, safe_text(txt), ln=True)

            pdf_path = f"/tmp/bq2_report_{run_id}.pdf"
            pdf.output(pdf_path)

            upload_to_s3_bq(payload["map_path"], "BQ2", "mismatch_map.html")
            upload_to_s3_bq(pdf_path, "BQ2", "mismatch_report.pdf")

        # ---- ENCADENADO ----
        out_bq2 = bq2_assets(bq2_extract())
        bq2_pdf_upload(out_bq2)

    # =========================================================
    # BQ3 — INDUSTRIAL MUNICIPALITIES (MISMA LÓGICA ORIGINAL)
    # =========================================================
    with TaskGroup("BQ3_Industrial_Municipalities"):

        # -----------------------------------------------------
        # 1. RANKING INDUSTRIAL (IDÉNTICO AL ORIGINAL)
        # -----------------------------------------------------
        @task
        def bq3_ranking(**context):
            p = context["params"]
            run_id = context["run_id"]
            gold = resolve_gold_table(p["tipo_zona"])
            con = get_con_and_attach()

            df_ranking = con.execute(f"""
                WITH daily_totals AS (
                    SELECT 
                        l.nombre, 
                        v.fecha, 
                        SUM(v.total_viajes) AS trips_per_day
                    FROM {gold} v
                    JOIN silver.lugares l ON v.id_destino = l.id
                    WHERE v.fecha BETWEEN '{p["start_date"]}' AND '{p["end_date"]}'
                    AND ST_Contains(ST_GeomFromText('{p["polygon"]}'), l.coordenadas)
                    GROUP BY 1, 2
                ),
                daily_avg AS (
                    SELECT 
                        nombre, 
                        c.tipo_dia, 
                        AVG(trips_per_day) AS avg_daily_vol
                    FROM daily_totals
                    JOIN silver.calendario c ON daily_totals.fecha = c.fecha
                    GROUP BY 1, 2
                ),
                stats AS (
                    SELECT 
                        nombre, 
                        AVG(CASE WHEN tipo_dia THEN avg_daily_vol END) AS workday_v,
                        AVG(CASE WHEN NOT tipo_dia THEN avg_daily_vol END) AS weekend_v
                    FROM daily_avg 
                    GROUP BY 1
                ),
                scored AS (
                    SELECT *, 
                        (workday_v / NULLIF(weekend_v, 0)) AS ratio,
                        PERCENT_RANK() OVER (
                            ORDER BY (workday_v / NULLIF(weekend_v, 0)) ASC
                        ) AS percentile
                    FROM stats 
                    WHERE workday_v > 50 AND weekend_v IS NOT NULL
                )
                SELECT * 
                FROM scored 
                WHERE percentile >= 0.95 
                ORDER BY ratio DESC;
            """).df()

            if df_ranking.empty:
                con.close()
                return {"empty": True}

            names_list = "'" + "','".join(df_ranking["nombre"].tolist()) + "'"

            con.close()
            return {
                "empty": False,
                "df_ranking": df_to_xcom(df_ranking),
                "names_list": names_list
            }

        # -----------------------------------------------------
        # 2. ACTIVOS (SERIE + MAPAS)
        # -----------------------------------------------------
        @task
        def bq3_assets(payload: dict, **context):
            if payload.get("empty"):
                return payload

            p = context["params"]
            run_id = context["run_id"]
            gold = resolve_gold_table(p["tipo_zona"])
            con = get_con_and_attach()

            df_ranking = df_from_xcom(payload["df_ranking"])
            names_list = payload["names_list"]

            df_hourly = con.execute(f"""
                SELECT 
                    CAST(v.periodo AS INTEGER) AS periodo,
                    c.tipo_dia,
                    SUM(v.total_viajes) AS total_viajes
                FROM {gold} v
                JOIN silver.lugares l ON v.id_destino = l.id
                JOIN silver.calendario c ON v.fecha = c.fecha
                WHERE l.nombre IN ({names_list})
                AND v.fecha BETWEEN '{p["start_date"]}' AND '{p["end_date"]}'
                GROUP BY 1, 2
                ORDER BY 1, 2
            """).df()

            plt.figure(figsize=(12, 6))
            sns.lineplot(
                data=df_hourly,
                x="periodo",
                y="total_viajes",
                hue="tipo_dia",
                marker="o"
            )
            plt.title("BQ3: Volumen Total Municipios Industriales")
            chart_path = f"/tmp/bq3_chart_{run_id}.png"
            plt.savefig(chart_path)
            plt.close()

            df_flows = con.execute(f"""
                SELECT 
                    mo.nombre AS origen,
                    md.nombre AS destino,
                    AVG(mo.latitude) AS o_lat,
                    AVG(mo.longitude) AS o_lon,
                    AVG(md.latitude) AS d_lat,
                    AVG(md.longitude) AS d_lon,
                    SUM(v.total_viajes) AS viajeros
                FROM {gold} v
                JOIN silver.lugares mo ON v.id_origen = mo.id
                JOIN silver.lugares md ON v.id_destino = md.id
                WHERE md.nombre IN ({names_list})
                AND v.fecha BETWEEN '{p["start_date"]}' AND '{p["end_date"]}'
                GROUP BY 1, 2
            """).df()

            df_points = con.execute(f"""
                SELECT 
                    l.nombre,
                    l.latitude,
                    l.longitude,
                    CASE 
                        WHEN l.nombre IN ({names_list})
                        THEN 'Top 5%'
                        ELSE 'Resto'
                    END AS grupo
                FROM silver.lugares l
                WHERE ST_Contains(
                    ST_GeomFromText('{p["polygon"]}'),
                    l.coordenadas
                )
            """).df()

            con.close()

            # Guardar mapas
            flow_map_path = f"/tmp/bq3_flows_{run_id}.html"
            KeplerGl(
                height=700,
                data={"Flujos": df_flows}
            ).save_to_html(file_name=flow_map_path)

            points_map_path = f"/tmp/bq3_points_{run_id}.html"
            KeplerGl(
                data={"Municipios": df_points}
            ).save_to_html(file_name=points_map_path)

            return {
                "df_ranking": payload["df_ranking"],
                "chart_path": chart_path,
                "flow_map_path": flow_map_path,
                "points_map_path": points_map_path
            }

        # -----------------------------------------------------
        # 3. PDF + UPLOAD (MISMO FORMATO ORIGINAL)
        # -----------------------------------------------------
        @task
        def bq3_pdf_upload(payload: dict, **context):
            if payload.get("empty"):
                return

            run_id = context["run_id"]
            df_ranking = df_from_xcom(payload["df_ranking"])

            pdf = FPDF()
            pdf.add_page()

            pdf.set_font("Arial", "B", 16)
            pdf.cell(
                0, 15,
                safe_text("BQ3: Reporte Industrial Consolidado"),
                ln=True,
                align="C"
            )

            pdf.ln(5)
            pdf.set_font("Arial", "B", 12)
            pdf.cell(0, 10, safe_text("Top 5% Municipios:"), ln=True)

            pdf.set_font("Arial", "", 10)
            for _, row in df_ranking.iterrows():
                pdf.cell(
                    0, 7,
                    safe_text(f"- {row['nombre']}: Ratio {row['ratio']:.2f}"),
                    ln=True
                )

            pdf.image(payload["chart_path"], x=10, w=190)

            pdf_path = f"/tmp/bq3_report_{run_id}.pdf"
            pdf.output(pdf_path)

            upload_to_s3_bq(pdf_path, "BQ3", "industrial_report.pdf")
            upload_to_s3_bq(payload["flow_map_path"], "BQ3", "industrial_flow_map.html")
            upload_to_s3_bq(payload["points_map_path"], "BQ3", "industrial_points_map.html")

        # -----------------------------------------------------
        # ENCADENADO
        # -----------------------------------------------------
        out_bq3 = bq3_assets(bq3_ranking())
        bq3_pdf_upload(out_bq3)

    return

dag = gold_bq_dag()

from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from pendulum import datetime
import duckdb
import os
import folium
import boto3

# ============================================================
# DuckDB / DuckLake init (MISMA ARQUITECTURA QUE BRONZE/SILVER)
# ============================================================

def init_duckdb(aws_conn, pg_conn):
    os.environ["PGSSLMODE"] = "require"

    con = duckdb.connect(database=":memory:")

    con.execute("SET ducklake_max_retry_count = 100;")
    con.execute("SET max_memory='2GB';")

    con.execute("""
        INSTALL ducklake;
        INSTALL postgres;
        INSTALL spatial;
    """)
    con.execute("""
        LOAD ducklake;
        LOAD spatial;
    """)

    con.execute(f"""
        SET s3_region='{aws_conn.extra_dejson.get("region", "eu-central-1")}';
        SET s3_access_key_id='{aws_conn.login}';
        SET s3_secret_access_key='{aws_conn.password}';
    """)

    db_name = pg_conn.schema or "neondb"

    con.execute(f"""
        CREATE OR REPLACE SECRET secreto_postgres (
            TYPE postgres,
            HOST '{pg_conn.host}',
            PORT {pg_conn.port},
            DATABASE '{db_name}',
            USER '{pg_conn.login}',
            PASSWORD '{pg_conn.password}'
        );
    """)

    con.execute("""
        CREATE OR REPLACE SECRET secreto_ducklake (
            TYPE ducklake,
            METADATA_PATH '',
            METADATA_PARAMETERS MAP {
                'TYPE': 'postgres',
                'SECRET': 'secreto_postgres'
            }
        );
    """)

    return con


def get_con_and_attach():
    aws_conn = BaseHook.get_connection("aws_s3_conn")
    pg_conn = BaseHook.get_connection("my_postgres_conn")

    con = init_duckdb(aws_conn, pg_conn)

    con.execute("""
        ATTACH 'ducklake:secreto_ducklake' AS lake (
            DATA_PATH 's3://pruebas-airflow-carlos/',
            OVERRIDE_DATA_PATH TRUE
        );
    """)
    con.execute("USE lake;")

    return con


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id="gold_analytics_poligono",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gold", "spatial", "analytics"]
)
def gold_analytics_poligono():

    # --------------------------------------------------------
    # 1. CREAR TABLA gravity_mismatch (FILTRADA POR POLÍGONO)
    # --------------------------------------------------------
    @task
    def compute_gravity_mismatch(**context):
        polygon_wkt = context["params"]["polygon"]

        con = get_con_and_attach()

        con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

        con.execute("DROP TABLE IF EXISTS gold.infrastructure_mismatch;")

        con.execute(f"""
            CREATE TABLE gold.infrastructure_mismatch AS
            WITH municipios_en_poligono AS (
                SELECT id
                FROM lake.silver.lugares
                WHERE ST_Contains(
                    ST_GeomFromText('{polygon_wkt}'),
                    coordenadas
                )
            ),

            actual AS (
                SELECT
                    id_origen,
                    id_destino,
                    SUM(viajes) AS trips,
                    AVG(viajes_metros) AS dist
                FROM lake.silver.viajes
                WHERE id_origen IN (SELECT id FROM municipios_en_poligono)
                OR id_destino IN (SELECT id FROM municipios_en_poligono)
                GROUP BY id_origen, id_destino
                HAVING dist > 2000
            ),

            potential AS (
                SELECT
                    a.*,
                    p.poblacion AS pop_o,
                    r.renta AS inc_d,
                    (CAST(p.poblacion AS BIGINT) * CAST(r.renta AS BIGINT))
                    / NULLIF(POWER(a.dist, 2), 0) AS gravity_score
                FROM actual a
                JOIN silver.poblacion p ON a.id_origen = p.id
                JOIN silver.renta r     ON a.id_destino = r.id
            ),

            results AS (
                SELECT
                    *,
                    trips / NULLIF(
                        gravity_score * (
                            SELECT SUM(trips) / SUM(gravity_score) FROM potential
                        ),
                        0
                    ) AS mismatch_ratio
                FROM potential
            )

            SELECT
                lo.nombre        AS municipio_origen,
                lo.coordenadas   AS geom_origen,
                ld.nombre        AS municipio_destino,
                ld.coordenadas   AS geom_destino,
                res.trips        AS viajes_reales,
                res.mismatch_ratio
            FROM results res
            LEFT JOIN lake.silver.lugares lo ON res.id_origen  = lo.id
            LEFT JOIN lake.silver.lugares ld ON res.id_destino = ld.id;

        """)

        con.close()

    # --------------------------------------------------------
    # 2. GENERAR MAPA Y GUARDAR EN S3
    # --------------------------------------------------------
    @task
    def generate_mismatch_map(**context):
        polygon_wkt = context["params"]["polygon"]
        run_id = context["run_id"]

        con = get_con_and_attach()

        df = con.execute("""
            SELECT
                municipio_origen,
                ST_Y(ST_Centroid(geom_origen)) AS lat_origen,
                ST_X(ST_Centroid(geom_origen)) AS lon_origen,
                municipio_destino,
                ST_Y(ST_Centroid(geom_destino)) AS lat_destino,
                ST_X(ST_Centroid(geom_destino)) AS lon_destino,
                viajes_reales,
                mismatch_ratio
            FROM gold.infrastructure_mismatch
            WHERE mismatch_ratio < 0.4
              AND viajes_reales > 500
              AND geom_origen IS NOT NULL
              AND geom_destino IS NOT NULL
        """).df()

        con.close()

        # ---------- MAPA ----------
        m = folium.Map(location=[40.4, -3.7], zoom_start=6, tiles="cartodbpositron")
        all_coords = []

        for _, row in df.iterrows():
            start = [row.lat_origen, row.lon_origen]
            end = [row.lat_destino, row.lon_destino]
            all_coords.extend([start, end])

            color = "red" if row.mismatch_ratio < 0.1 else "orange"
            weight = (row.viajes_reales / 20000) + 2

            folium.PolyLine(
                [start, end],
                color=color,
                weight=weight,
                opacity=0.6,
                tooltip=(
                    f"{row.municipio_origen} → {row.municipio_destino}<br>"
                    f"Ratio: {round(row.mismatch_ratio, 3)}<br>"
                    f"Viajes: {int(row.viajes_reales)}"
                )
            ).add_to(m)

        if all_coords:
            m.fit_bounds(all_coords)

        local_path = f"/tmp/mismatch_map_{run_id}.html"
        m.save(local_path)

        # ---------- SUBIR A S3 ----------
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name=aws_conn.extra_dejson.get("region", "eu-central-1"),
        )

        s3.upload_file(
            local_path,
            "pruebas-airflow-carlos",
            f"gold/report/mismatch_map_{run_id}.html"
        )

        os.remove(local_path)

    compute = compute_gravity_mismatch()
    compute >> generate_mismatch_map()


dag = gold_analytics_poligono()



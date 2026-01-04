from airflow.sdk import dag, task, Param
from airflow.sdk.bases.hook import BaseHook
from pendulum import datetime
import duckdb
import os
import folium
import boto3

# ============================================================
# CONFIGURACIÓN DE DUCKDB / DUCKLAKE
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
        LOAD postgres;
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
            DATA_PATH 's3://pruebas-airflow-carlos2/',
            OVERRIDE_DATA_PATH TRUE
        );
    """)
    con.execute("USE lake;")
    return con

# ============================================================
# DEFINICIÓN DEL DAG
# ============================================================

@dag(
    dag_id="gold_analytics_poligono_final",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gold", "spatial", "manual_input"],
    params={
        "polygon": Param(
            "POLYGON ((-1.494141 39.850721, -1.538086 39.164141, -0.230713 39.164141, -0.263672 39.951859, -1.494141 39.850721))",
            type="string",
            title="Polígono de estudio (WKT)"
        )
    }
)
def gold_analytics_dag():

    @task
    def inspect_schema():
        con = get_con_and_attach()
        print("--- [LOG] TABLAS EN SILVER ---")
        print(con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'silver';").df())
        print("\n--- [LOG] COLUMNAS DE LOCATIONS ---")
        # Corregido: Ahora apunta a 'locations' en lugar de 'lugares'
        try:
            print(con.execute("DESCRIBE silver.locations;").df())
        except Exception as e:
            print(f"Error al describir tabla: {e}")
        con.close()

    @task
    def compute_mismatch(**context):
        polygon_wkt = context["params"]["polygon"]
        con = get_con_and_attach()

        con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
        con.execute("DROP TABLE IF EXISTS gold.infrastructure_mismatch;")

        # Corregido: Cambio de 'lugares' a 'locations' en los JOIN y subconsultas
        con.execute(f"""
            CREATE TABLE gold.infrastructure_mismatch AS
            WITH municipios_en_poligono AS (
                SELECT id
                FROM lake.silver.locations
                WHERE ST_Contains(
                    ST_GeomFromText('{polygon_wkt}'),
                    coordenadas
                )
            ),
            actual AS (
                SELECT
                    id_origen, id_destino,
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
                    a.*, p.poblacion AS pop_o, r.renta AS inc_d,
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
                        gravity_score * (SELECT SUM(trips)/SUM(gravity_score) FROM potential),
                        0
                    ) AS mismatch_ratio
                FROM potential
            )
            SELECT
                lo.nombre AS municipio_origen, lo.coordenadas AS geom_origen,
                ld.nombre AS municipio_destino, ld.coordenadas AS geom_destino,
                res.trips AS viajes_reales, res.mismatch_ratio
            FROM results res
            LEFT JOIN lake.silver.locations lo ON res.id_origen  = lo.id
            LEFT JOIN lake.silver.locations ld ON res.id_destino = ld.id;
        """)
        con.close()

    @task
    def generate_map(**context):
        run_id = context["run_id"]
        con = get_con_and_attach()

        df = con.execute("""
            SELECT
                municipio_origen,
                ST_Y(ST_Centroid(geom_origen)) AS lat_o, ST_X(ST_Centroid(geom_origen)) AS lon_o,
                municipio_destino,
                ST_Y(ST_Centroid(geom_destino)) AS lat_d, ST_X(ST_Centroid(geom_destino)) AS lon_d,
                viajes_reales, mismatch_ratio
            FROM gold.infrastructure_mismatch
            WHERE mismatch_ratio < 0.4 AND viajes_reales > 500
        """).df()
        con.close()

        m = folium.Map(location=[40.4, -3.7], zoom_start=6, tiles="cartodbpositron")
        all_coords = []
        for _, row in df.iterrows():
            start, end = [row.lat_o, row.lon_o], [row.lat_d, row.lon_d]
            all_coords.extend([start, end])
            color = "red" if row.mismatch_ratio < 0.1 else "orange"
            folium.PolyLine(
                [start, end], color=color, weight=(row.viajes_reales/20000)+2, opacity=0.6,
                tooltip=f"Origen: {row.municipio_origen}<br>Ratio: {round(row.mismatch_ratio, 3)}"
            ).add_to(m)

        if all_coords: m.fit_bounds(all_coords)
        local_path = f"/tmp/map_{run_id}.html"
        m.save(local_path)

        aws_conn = BaseHook.get_connection("aws_s3_conn")
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name=aws_conn.extra_dejson.get("region", "eu-central-1"),
        )
        s3.upload_file(local_path, "pruebas-airflow-carlos2", f"gold/report/mismatch_map_{run_id}.html")
        os.remove(local_path)

    inspect_schema() >> compute_mismatch() >> generate_map()

gold_dag = gold_analytics_dag()

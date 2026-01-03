from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from pendulum import datetime
from datetime import timedelta
import duckdb

# ===============================================================
# Inicialización DuckDB + DuckLake (Cambiado a 'lake')
# ===============================================================
def init_duckdb(aws_conn, pg_conn):
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL ducklake; INSTALL postgres; INSTALL spatial;")
    con.execute("LOAD ducklake; LOAD spatial; LOAD postgres;")
    
    con.execute(f"""
        SET s3_region           = 'eu-central-1'; 
        SET s3_access_key_id     = '{aws_conn.login}'; 
        SET s3_secret_access_key = '{aws_conn.password}';
    """)
    
    con.execute(f"""
        CREATE OR REPLACE SECRET secreto_postgres (
            TYPE     POSTGRES, 
            HOST     '{pg_conn.host}', 
            PORT     {pg_conn.port}, 
            DATABASE '{pg_conn.schema}', 
            USER     '{pg_conn.login}', 
            PASSWORD '{pg_conn.password}'
        );
    """)
    
    con.execute("""
        CREATE OR REPLACE SECRET secreto_ducklake (
            TYPE                DUCKLAKE, 
            METADATA_PATH       '', 
            METADATA_PARAMETERS MAP {
                'TYPE':   'postgres', 
                'SECRET': 'secreto_postgres'
            }
        );
    """)
    return con

def get_2023_full_year_paths():
    base = "s3://dl-mobility-spain/audit/viajes/municipios/2023"
    suffix = "Viajes_municipios"
    days_per_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    return [
        f"{base}/{month:02d}/2023{month:02d}{day:02d}_{suffix}.csv.gz"
        for month, num_days in enumerate(days_per_month, start=1)
        for day in range(1, num_days + 1)
    ]

@dag(
    dag_id="mobility_BATCH_FULL_YEAR_2023",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "silver", "aws_batch"]
)
def mobility_ingestion():

    # ===========================================================
    # 1. BRONZE LOCAL: CONFIGURACIÓN INICIAL (Uso de 'lake')
    # ===========================================================

    @task
    def ingest_bronze_official_data():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)
        
        # ALIAS 'lake' para consistencia con Gold y duckrunner
        con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS lake (DATA_PATH 's3://pruebas-airflow-carlos/', OVERRIDE_DATA_PATH TRUE);")
        con.execute("CREATE SCHEMA IF NOT EXISTS lake.bronze;")

        con.execute("""
            CREATE OR REPLACE TABLE lake.bronze.renta AS 
            SELECT * FROM read_csv_auto(
                'https://www.ine.es/jaxiT3/files/t/es/csv_bd/30824.csv?nocab=1', 
                sep    = '\\t', 
                header = true
            );
        """)

        con.execute("""
            CREATE OR REPLACE TABLE lake.bronze.poblacion AS 
            SELECT * FROM read_csv_auto(
                'https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/poblacion_municipios.csv', 
                sep    = '|', 
                header = false
            );
        """)

        con.execute("""
            CREATE OR REPLACE TABLE lake.bronze.geo_municipios AS 
            SELECT * FROM ST_Read(
                'https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/zonificacion_municipios.shp'
            );
        """)

        con.execute("""
            CREATE OR REPLACE TABLE lake.bronze.relaciones AS 
            SELECT * FROM read_csv_auto(
                'https://movilidad-opendata.mitma.es/zonificacion/relacion_ine_zonificacionMitma.csv', 
                sep    = '|', 
                header = true
            );
        """)
        con.close()

    @task
    def create_viajes_bronze_table():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)
        con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS lake (DATA_PATH 's3://pruebas-airflow-carlos/', OVERRIDE_DATA_PATH TRUE);")
        
        con.execute("""
            CREATE OR REPLACE TABLE lake.bronze.trips_municipios_2023 AS 
            SELECT * FROM read_csv_auto(
                's3://dl-mobility-spain/audit/viajes/municipios/2023/01/20230101_Viajes_municipios.csv.gz'
            ) LIMIT 0;
        """)
        con.close()

    # ===========================================================
    # 2. BRONZE BATCH: CARGA MASIVA (Uso de 'lake' en el Job)
    # ===========================================================

    batch_overrides = [
        {
            "containerOverrides": {
                "environment": [
                    {
                        "name": "SQL_QUERY", 
                        "value": f"INSERT INTO lake.bronze.trips_municipios_2023 SELECT * FROM read_csv('{fp}', header=true, auto_detect=true);"
                    },
                    {"name": "memory", "value": "2GB"}
                ]
            }
        }
        for fp in get_2023_full_year_paths()
    ]

    load_viajes_batch = BatchOperator.partial(
        task_id         = "load_viajes_batch",
        job_name        = "ingest_trip_day",
        job_queue       = "DuckJobQueue",
        job_definition  = "EC2JobDefinition:2", 
        region_name     = "eu-central-1"
    ).expand(overrides=batch_overrides)

    # ===========================================================
    # 3. SILVER: TRANSFORMACIÓN FINAL (Uso de 'lake')
    # ===========================================================

    @task
    def setup_silver_schema():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)
        con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS lake (DATA_PATH 's3://pruebas-airflow-carlos/', OVERRIDE_DATA_PATH TRUE);")
        con.execute("CREATE SCHEMA IF NOT EXISTS lake.silver;")
        con.close()

    @task
    def create_silver_dimensions():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)
        con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS lake (DATA_PATH 's3://pruebas-airflow-carlos/', OVERRIDE_DATA_PATH TRUE);")
        con.execute("USE lake;")

        con.execute("""
            CREATE OR REPLACE TABLE silver.poblacion AS 
            SELECT 
                CAST(SUBSTR(TRIM(column0), 1, 5) AS INTEGER) AS id, 
                CAST(REPLACE(column1, ',', '') AS INTEGER)  AS poblacion 
            FROM bronze.poblacion 
            WHERE TRY_CAST(column1 AS INTEGER) IS NOT NULL;
        """)

        con.execute("""
            CREATE OR REPLACE TABLE silver.renta AS 
            SELECT 
                CAST(SUBSTR(TRIM(column0), 1, 5) AS INTEGER)             AS id, 
                CAST(REPLACE("Renta neta media por persona", '.', '') AS INTEGER) AS renta 
            FROM bronze.renta 
            WHERE Periodo = '2023';
        """)

        con.execute("""
            CREATE OR REPLACE TABLE silver.lugares AS
            SELECT 
                CAST(r.municipio AS INTEGER)      AS id, 
                r.municipio_mitma                 AS id_mitma,
                g.NAME                            AS nombre, 
                g.geom                            AS coordenadas, 
                'municipio'                       AS tipo_zona,
                ST_X(ST_Centroid(g.geom))         AS longitude, 
                ST_Y(ST_Centroid(g.geom))         AS latitude
            FROM bronze.relaciones r
            JOIN bronze.geo_municipios g ON TRIM(r.municipio_mitma) = TRIM(g.ID);
        """)
        con.close()

    @task
    def populate_silver_viajes():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)
        con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS lake (DATA_PATH 's3://pruebas-airflow-carlos/', OVERRIDE_DATA_PATH TRUE);")
        
        con.execute("""
            CREATE OR REPLACE TABLE lake.silver.viajes AS
            SELECT 
                CAST(STRPTIME(CAST(fecha AS VARCHAR), '%Y%m%d') AS DATE) AS fecha,
                CAST(periodo AS INTEGER)         AS periodo, 
                CAST(origen AS INTEGER)          AS id_origen,
                CAST(destino AS INTEGER)         AS id_destino, 
                CAST(viajes AS INTEGER)          AS viajes,
                CAST(viajes_km * 1000 AS DOUBLE) AS viajes_metros
            FROM lake.bronze.trips_municipios_2023 
            WHERE viajes_km IS NOT NULL;
        """)
        con.close()

    # Dependencias
    b_off = ingest_bronze_official_data()
    v_tab = create_viajes_bronze_table()
    s_set = setup_silver_schema()
    s_dim = create_silver_dimensions()
    s_via = populate_silver_viajes()

    b_off >> v_tab >> load_viajes_batch >> s_set >> s_dim >> s_via

mobility_dag = mobility_ingestion()

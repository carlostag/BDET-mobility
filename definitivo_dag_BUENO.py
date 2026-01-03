from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.batch_client import BatchHook
from pendulum import datetime
from datetime import timedelta
import duckdb

# ===============================================================
# Inicialización DuckDB + DuckLake (Catálogo 'lake')
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
    dag_id="mobility_BATCH_FULL_YEAR_2023_v3",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "silver", "aws_batch", "airflow_3"]
)
def mobility_ingestion():

    # ===========================================================
    # 1. BRONZE LOCAL: CONFIGURACIÓN INICIAL
    # ===========================================================

    @task
    def ingest_bronze_official_data():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)
        con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS lake (DATA_PATH 's3://pruebas-airflow-carlos/', OVERRIDE_DATA_PATH TRUE);")
        con.execute("CREATE SCHEMA IF NOT EXISTS lake.bronze;")

        # Cargamos dimensiones oficiales como VARCHAR para evitar errores de tipo
        con.execute("""
            CREATE OR REPLACE TABLE lake.bronze.renta AS 
            SELECT * FROM read_csv_auto('https://www.ine.es/jaxiT3/files/t/es/csv_bd/30824.csv?nocab=1', sep='\\t', all_varchar=true);
        """)
        con.execute("""
            CREATE OR REPLACE TABLE lake.bronze.poblacion AS 
            SELECT * FROM read_csv_auto('https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/poblacion_municipios.csv', sep='|', all_varchar=true);
        """)
        con.execute("""
            CREATE OR REPLACE TABLE lake.bronze.geo_municipios AS 
            SELECT * FROM ST_Read('https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/zonificacion_municipios.shp');
        """)
        con.execute("""
            CREATE OR REPLACE TABLE lake.bronze.relaciones AS 
            SELECT * FROM read_csv_auto('https://movilidad-opendata.mitma.es/zonificacion/relacion_ine_zonificacionMitma.csv', sep='|', all_varchar=true);
        """)
        con.close()

    @task
    def create_viajes_bronze_table_init():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)
        con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS lake (DATA_PATH 's3://pruebas-airflow-carlos/', OVERRIDE_DATA_PATH TRUE);")
        
        # Idempotencia: tabla vacía inicial
        con.execute("""
            CREATE OR REPLACE TABLE lake.bronze.trips_municipios_2023 AS 
            SELECT * FROM read_csv_auto(
                's3://dl-mobility-spain/audit/viajes/municipios/2023/01/20230101_Viajes_municipios.csv.gz',
                all_varchar = true
            ) LIMIT 0;
        """)
        con.close()

    # ===========================================================
    # 2. BRONZE BATCH: ENVÍO DINÁMICO (Compatible con Airflow 3)
    # ===========================================================

    @task
    def submit_batch_ingestion(file_path: str):
        hook = BatchHook(region_name="eu-central-1")
        
        # SQL con all_varchar=true para evitar el error "si/no" -> BOOLEAN
        sql = f"INSERT INTO lake.bronze.trips_municipios_2023 SELECT * FROM read_csv('{file_path}', header=true, all_varchar=true);"
        
        response = hook.submit_job(
            job_name       = "ingest_trip_day",
            job_queue      = "DuckJobQueue",
            job_definition = "EC2JobDefinition:2",
            containerOverrides = {
                "environment": [
                    {"name": "SQL_QUERY", "value": sql},
                    {"name": "memory",    "value": "2GB"}
                ]
            }
        )
        return response['jobId']

    # Mapeo dinámico de los 365 días
    batch_jobs = submit_batch_ingestion.expand(file_path=get_2023_full_year_paths())

    # ===========================================================
    # 3. SILVER: TRANSFORMACIÓN Y LIMPIEZA (CAST DE TIPOS)
    # ===========================================================

    @task
    def run_silver_transformation(wait_for_jobs):
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)
        con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS lake (DATA_PATH 's3://pruebas-airflow-carlos/', OVERRIDE_DATA_PATH TRUE);")
        con.execute("CREATE SCHEMA IF NOT EXISTS lake.silver; USE lake;")

        # Silver Población: Limpiamos el texto y convertimos a INTEGER
        con.execute("""
            CREATE OR REPLACE TABLE silver.poblacion AS 
            SELECT CAST(SUBSTR(TRIM(column0), 1, 5) AS INTEGER) AS id, 
                   CAST(REPLACE(column1, ',', '') AS INTEGER)  AS poblacion 
            FROM bronze.poblacion WHERE TRY_CAST(REPLACE(column1, ',', '') AS INTEGER) IS NOT NULL;
        """)

        # Silver Renta: Limpiamos puntos y filtramos año
        con.execute("""
            CREATE OR REPLACE TABLE silver.renta AS 
            SELECT CAST(SUBSTR(TRIM(column0), 1, 5) AS INTEGER) AS id, 
                   CAST(REPLACE("Renta neta media por persona", '.', '') AS INTEGER) AS renta 
            FROM bronze.renta WHERE Periodo = '2023';
        """)

        # Silver Lugares: Unión de Geo y Relaciones
        con.execute("""
            CREATE OR REPLACE TABLE silver.lugares AS
            SELECT CAST(r.municipio AS INTEGER) AS id, r.municipio_mitma AS id_mitma,
                   g.NAME AS nombre, g.geom AS coordenadas, 'municipio' AS tipo_zona,
                   ST_X(ST_Centroid(g.geom)) AS longitude, ST_Y(ST_Centroid(g.geom)) AS latitude
            FROM bronze.relaciones r
            JOIN bronze.geo_municipios g ON TRIM(r.municipio_mitma) = TRIM(g.ID);
        """)

        # Silver Viajes: Conversión final de la carga masiva de Batch
        con.execute("""
            CREATE OR REPLACE TABLE silver.viajes AS
            SELECT CAST(STRPTIME(CAST(fecha AS VARCHAR), '%Y%m%d') AS DATE) AS fecha,
                   CAST(periodo AS INTEGER) AS periodo, CAST(origen AS INTEGER) AS id_origen,
                   CAST(destino AS INTEGER) AS id_destino, CAST(viajes AS INTEGER) AS viajes,
                   CAST(viajes_km * 1000 AS DOUBLE) AS viajes_metros
            FROM bronze.trips_municipios_2023 WHERE viajes_km IS NOT NULL;
        """)
        con.close()

    # Orquestación
    bronze_setup = ingest_bronze_official_data()
    v_table_init = create_viajes_bronze_table_init()
    
    # La transformación Silver espera a que terminen los 365 envíos a Batch
    silver_task = run_silver_transformation(wait_for_jobs=batch_jobs)

    bronze_setup >> v_table_init >> batch_jobs >> silver_task

mobility_dag = mobility_ingestion()

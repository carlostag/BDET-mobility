from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from pendulum import datetime
from datetime import timedelta
import duckdb

# ===============================================================
# Inicialización DuckDB + DuckLake (Sin cambios)
# ===============================================================
def init_duckdb(aws_conn, pg_conn):
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL ducklake; INSTALL postgres; INSTALL spatial;")
    con.execute("LOAD ducklake; LOAD spatial; LOAD postgres;")
    con.execute(f"SET s3_region='eu-central-1'; SET s3_access_key_id='{aws_conn.login}'; SET s3_secret_access_key='{aws_conn.password}';")
    con.execute(f"CREATE OR REPLACE SECRET secreto_postgres (TYPE postgres, HOST '{pg_conn.host}', PORT {pg_conn.port}, DATABASE '{pg_conn.schema}', USER '{pg_conn.login}', PASSWORD '{pg_conn.password}');")
    con.execute("CREATE OR REPLACE SECRET secreto_ducklake (TYPE ducklake, METADATA_PATH '', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'secreto_postgres'});")
    return con

# NUEVA UTILIDAD: Genera las rutas para los 365 días del año 2023
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
    dag_id="mobility_FULL_YEAR_2023",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "silver", "official_sources"]
)
def mobility_ingestion():

    @task
    def ingest_bronze_official_data():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)
        con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS movilidad (DATA_PATH 's3://pruebas-airflow-carlos/', OVERRIDE_DATA_PATH TRUE);")
        con.execute("CREATE SCHEMA IF NOT EXISTS movilidad.bronze;")
        
        sources = {
            "renta": "https://www.ine.es/jaxiT3/files/t/es/csv_bd/30824.csv?nocab=1",
            "poblacion": "https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/poblacion_municipios.csv",
            "relaciones": "https://movilidad-opendata.mitma.es/zonificacion/relacion_ine_zonificacionMitma.csv",
            "geo_municipios": "https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/zonificacion_municipios.shp",
            "provincias": "https://movilidad-opendata.mitma.es/zonificacion/zonificacion_provincias/zonificacion_provincias.shp"
        }
        
        con.execute(f"CREATE OR REPLACE TABLE movilidad.bronze.renta AS SELECT * FROM read_csv_auto('{sources['renta']}', sep='\\t', header=true);")
        con.execute(f"CREATE OR REPLACE TABLE movilidad.bronze.poblacion AS SELECT * FROM read_csv_auto('{sources['poblacion']}', sep='|', header=false);")
        con.execute(f"CREATE OR REPLACE TABLE movilidad.bronze.relaciones AS SELECT * FROM read_csv_auto('{sources['relaciones']}', sep='|', header=true);")
        con.execute(f"CREATE OR REPLACE TABLE movilidad.bronze.geo_municipios AS SELECT * FROM ST_Read('{sources['geo_municipios']}');")
        con.execute(f"CREATE OR REPLACE TABLE movilidad.bronze.provincias AS SELECT * FROM ST_Read('{sources['provincias']}');")
        con.close()

    @task
    def create_viajes_bronze_table():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)
        con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS movilidad (DATA_PATH 's3://pruebas-airflow-carlos/', OVERRIDE_DATA_PATH TRUE);")
        con.execute("CREATE OR REPLACE TABLE movilidad.bronze.trips_municipios_2023 AS SELECT * FROM read_csv_auto('s3://dl-mobility-spain/audit/viajes/municipios/2023/01/20230101_Viajes_municipios.csv.gz') LIMIT 0;")
        con.close()

    # IMPORTANTE: max_active_tis_per_dag=1 evita que la t3.small se quede sin RAM
    @task(max_active_tis_per_dag=1, retries=3)
    def load_viajes_bronze_files(file_path: str):
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)
        con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS movilidad (DATA_PATH 's3://pruebas-airflow-carlos/', OVERRIDE_DATA_PATH TRUE);")
        con.execute(f"INSERT INTO movilidad.bronze.trips_municipios_2023 SELECT * FROM read_csv('{file_path}', header=true, auto_detect=true);")
        con.close()

    @task
    def setup_silver():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)
        con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS movilidad (DATA_PATH 's3://pruebas-airflow-carlos/', OVERRIDE_DATA_PATH TRUE);")
        con.execute("CREATE SCHEMA IF NOT EXISTS movilidad.silver;")
        con.close()

    @task
    def create_silver_tables():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)
        con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS movilidad (DATA_PATH 's3://pruebas-airflow-carlos/', OVERRIDE_DATA_PATH TRUE);")
        con.execute("USE movilidad;")
        con.execute("CREATE OR REPLACE TABLE silver.provincias AS SELECT CAST(Codigo AS INTEGER) AS cod_provincia, TRIM(Texto) AS nombre_provincia, geom FROM bronze.provincias;")
        con.execute("CREATE OR REPLACE TABLE silver.poblacion AS SELECT CAST(SUBSTR(TRIM(column0), 1, 5) AS INTEGER) AS id, CAST(REPLACE(column1, ',', '') AS INTEGER) AS poblacion FROM bronze.poblacion WHERE TRY_CAST(column1 AS INTEGER) IS NOT NULL;")
        con.execute("CREATE OR REPLACE TABLE silver.renta AS SELECT CAST(SUBSTR(TRIM(column0), 1, 5) AS INTEGER) AS id, CAST(REPLACE(\"Renta neta media por persona\", '.', '') AS INTEGER) AS renta FROM bronze.renta WHERE Periodo = '2023';")
        con.execute("CREATE OR REPLACE TABLE silver.lugares AS SELECT CAST(r.municipio AS INTEGER) AS id, r.municipio_mitma AS id_mitma, g.NAME AS nombre, g.geom AS coordenadas, 'municipio' AS tipo_zona, ST_X(ST_Centroid(g.geom)) AS longitude, ST_Y(ST_Centroid(g.geom)) AS latitude FROM bronze.relaciones r JOIN bronze.geo_municipios g ON TRIM(r.municipio_mitma) = TRIM(g.ID);")
        con.close()

    @task
    def populate_silver_viajes():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)
        con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS movilidad (DATA_PATH 's3://pruebas-airflow-carlos/', OVERRIDE_DATA_PATH TRUE);")
        con.execute("CREATE OR REPLACE TABLE movilidad.silver.viajes AS SELECT CAST(STRPTIME(CAST(fecha AS VARCHAR), '%Y%m%d') AS DATE) AS fecha, CAST(periodo AS INTEGER) AS periodo, CAST(origen AS INTEGER) AS id_origen, CAST(destino AS INTEGER) AS id_destino, CAST(viajes AS INTEGER) AS viajes, CAST(viajes_km * 1000 AS DOUBLE) AS viajes_metros FROM movilidad.bronze.trips_municipios_2023 WHERE viajes_km IS NOT NULL;")
        con.close()

    # Orquestación
    bronze_official = ingest_bronze_official_data()
    cvbt = create_viajes_bronze_table()
    
    with TaskGroup("load_365_days") as load_trips:
        load_viajes_bronze_files.expand(file_path=get_2023_full_year_paths())

    setup = setup_silver()
    silver_static = create_silver_tables()
    silver_trips = populate_silver_viajes()

    bronze_official >> cvbt >> load_trips >> setup >> silver_static >> silver_trips

mobility_ingestion_dag = mobility_ingestion()

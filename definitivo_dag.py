
from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.utils.task_group import TaskGroup
from pendulum import datetime
from datetime import date, timedelta

import duckdb

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from urllib.parse import urlparse


def list_all_s3_files(s3_prefix: str, name_contains: str) -> list[str]:
    """
    Devuelve todos los ficheros S3 que contengan `name_contains`
    dentro de un prefijo S3 (s3://bucket/path)
    """
    parsed = urlparse(s3_prefix)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")

    hook = S3Hook(aws_conn_id="aws_s3_conn")

    keys = hook.list_keys(
        bucket_name=bucket,
        prefix=prefix
    )

    if not keys:
        raise ValueError(f"No files found in {s3_prefix}")

    return [
        f"s3://{bucket}/{key}"
        for key in keys
        if name_contains in key and key.endswith(".csv.gz")
    ]

def get_con_and_attach():
    aws_conn = BaseHook.get_connection("aws_s3_conn")
    pg_conn = BaseHook.get_connection("my_postgres_conn")

    con = init_duckdb(aws_conn, pg_conn)

    con.execute("""
        ATTACH 'ducklake:secreto_ducklake' AS movilidad (
            DATA_PATH 's3://mobility-shared-2/',
            OVERRIDE_DATA_PATH TRUE
        );
    """)

    con.execute("USE movilidad;")

    return con

# ===============================================================
# Inicialización DuckDB + DuckLake 
# ===============================================================
def init_duckdb(aws_conn, pg_conn):
    con = duckdb.connect(database=":memory:")

    con.execute("INSTALL ducklake;")
    con.execute("INSTALL postgres;")
    con.execute("INSTALL spatial;")

    con.execute("LOAD ducklake;")
    con.execute("LOAD spatial;")

    con.execute(f"""
        SET s3_region='{aws_conn.extra_dejson.get("region", "eu-central-1")}';
        SET s3_access_key_id='{aws_conn.login}';
        SET s3_secret_access_key='{aws_conn.password}';
    """)

    con.execute(f"""
        CREATE OR REPLACE SECRET secreto_postgres (
            TYPE postgres,
            HOST '{pg_conn.host}',
            PORT {pg_conn.port},
            DATABASE '{pg_conn.schema}',
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


# ===============================================================
# Utilidad: primeros 10 días de enero
# ===============================================================
def january_first_10_files(base_path: str, suffix: str):
    return [
        f"{base_path}/202301{day:02d}_{suffix}.csv.gz"
        for day in range(1, 11)
    ]


@dag(dag_id="mobility_bronze_silver",  start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "duckdb", "ducklake"]
)
def mobility_bronze_silver():
    @task(max_active_tis_per_dag=1, retries=5,
    retry_delay=timedelta(minutes=2))
    def create_bronze_table(table_name: str, sample_file: str):
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")

        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)
        con.execute("USE movilidad;")
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

        con.execute(f"""
            CREATE OR REPLACE TABLE bronze.{table_name} AS
            SELECT *
            FROM read_csv_auto(
                '{sample_file}',
                SAMPLE_SIZE = -1
            )
            LIMIT 0;
        """)

        con.close()

    @task(max_active_tis_per_dag=10, retries=5,
    retry_delay=timedelta(minutes=2))
    def load_bronze_file(file_path: str, table_name: str):
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")

        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)
        con.execute("USE movilidad;")

        con.execute(f"""
            INSERT INTO bronze.{table_name}
            SELECT *
            FROM read_csv(
                '{file_path}',
                HEADER = true,
                AUTO_DETECT = true
            );
        """)

        con.close()

    # -------------------- DISTRITOS --------------------
    create_distritos = create_bronze_table(
        table_name="trips_distritos_2023",
        sample_file="s3://mobility-shared/prebronze/viajes/distritos/2023/01/20230101_Viajes_distritos.csv.gz"
    )

    with TaskGroup(group_id="insert_distritos") as insert_distritos:
        load_bronze_file.partial(
            table_name="trips_distritos_2023"
        ).expand(
            file_path=list_all_s3_files(
                "s3://mobility-shared/prebronze/viajes/distritos/2023/",
                "Viajes_distritos"
            )
        )

    create_distritos >> insert_distritos

   # -------------------- MUNICIPIOS --------------------
    create_municipios = create_bronze_table(
        table_name="trips_municipios_2023",
        sample_file="s3://mobility-shared/prebronze/viajes/municipios/2023/01/20230101_Viajes_municipios.csv.gz"
    )

    with TaskGroup(group_id="insert_municipios") as insert_municipios:
        load_bronze_file.partial(
            table_name="trips_municipios_2023"
        ).expand(
            file_path=list_all_s3_files(
                "s3://mobility-shared/prebronze/viajes/municipios/2023/",
                "Viajes_municipios"
            )
        )

    create_municipios >> insert_municipios
 
    # -------------------- gaus --------------------
    create_gaus = create_bronze_table(
        table_name="trips_gaus_2023",
        sample_file="s3://mobility-shared/prebronze/viajes/gaus/2023/01/20230101_Viajes_GAU.csv.gz"
    )

    with TaskGroup(group_id="insert_gaus") as insert_gaus:
        load_bronze_file.partial(
            table_name="trips_gaus_2023"
        ).expand(
            file_path=list_all_s3_files(
                "s3://mobility-shared/prebronze/viajes/gaus/2023/",
                "Viajes_GAU"
            )
        )

    create_gaus >> insert_gaus

    # ===========================================================
    # ================== TABLAS ESTÁTICAS =======================
    # ===========================================================

    @task
    def start_bronze_static():
        pass

    @task(max_active_tis_per_dag=1, retries=5,
    retry_delay=timedelta(minutes=2))
    def load_renta():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)
        con.execute("USE movilidad;")
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

        con.execute("""
            CREATE OR REPLACE TABLE bronze.renta AS
            SELECT *
            FROM read_csv(
                's3://mobility-shared/prebronze/renta/2023/renta.csv',
                HEADER = true,
                AUTO_DETECT = true
            );
        """)

        con.close()

    @task(max_active_tis_per_dag=1, retries=5,
    retry_delay=timedelta(minutes=2))
    def load_poblacion():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)
        con.execute("USE movilidad;")
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

        con.execute("""
            CREATE OR REPLACE TABLE bronze.poblacion AS
            SELECT *
            FROM read_csv(
                's3://mobility-shared/prebronze/poblacion/2023/poblacion.csv',
                HEADER = true,
                AUTO_DETECT = true
            );
        """)

        con.close()

    @task(max_active_tis_per_dag=1, retries=5,
    retry_delay=timedelta(minutes=2))
    def load_relaciones():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)
        con.execute("USE movilidad;")
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

        con.execute(f"""
            CREATE OR REPLACE TABLE bronze.relaciones AS 
            SELECT * FROM read_csv('s3://dl-mobility-spain/audit/relacion/relacion_ine_zonificacionMitma.csv', 
                                sep='|', header=True, ALL_VARCHAR=TRUE);
        """)

        con.close()

    @task(max_active_tis_per_dag=1, retries=5,
    retry_delay=timedelta(minutes=2))
    def load_geo_table(table_name: str, geojson_path: str):
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)
        con.execute("USE movilidad;")

        con.execute(f"""
            CREATE OR REPLACE TABLE bronze.{table_name} AS
            SELECT *
            FROM ST_Read('{geojson_path}');
        """)

        con.close()

    @task(max_active_tis_per_dag=1, retries=5,
    retry_delay=timedelta(minutes=2))
    def load_provincias():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)
        con.execute("USE movilidad;")

        con.execute("""
            CREATE OR REPLACE TABLE bronze.provincias AS
            SELECT *
            FROM ST_Read('s3://mobility-shared/prebronze/geo/provincias/provincias.geojson');
        """)

        con.close()

    @task(max_active_tis_per_dag=1, retries=5,
    retry_delay=timedelta(minutes=2))
    def load_calendario():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)
        con.execute("USE movilidad;")

        con.execute("""
            CREATE OR REPLACE TABLE bronze.calendario AS
            SELECT *
            FROM read_csv(
                's3://mobility-shared/prebronze/calendario/2023/calendario_def.csv',
                HEADER = true,
                AUTO_DETECT = true
            );
        """)

        con.close()

    # ===========================================================
    # ===================== DEPENDENCIAS ========================
    # ===========================================================

    start = start_bronze_static()

    renta = load_renta()
    poblacion = load_poblacion()
    relaciones = load_relaciones()

    geo_municipios = load_geo_table(
        table_name="geo_municipios",
        geojson_path="s3://mobility-shared/prebronze/geo/municipios/municipios.geojson")
    
    geo_distritos = load_geo_table(
        table_name="geo_distritos",
        geojson_path="s3://mobility-shared/prebronze/geo/distritos/distritos.geojson"
    )
    geo_gaus = load_geo_table(
        table_name="geo_gaus",
        geojson_path="s3://mobility-shared/prebronze/geo/gaus/gaus.geojson"
    )

    provincias = load_provincias()
    calendario = load_calendario()

    start >> [
        renta,
        poblacion,
        relaciones,
        geo_municipios,
        geo_distritos,
        geo_gaus,
        provincias,
        calendario
    ]

    @task(
        max_active_tis_per_dag=1,
        retries=3,
        retry_delay=timedelta(minutes=2)
    )
    def setup_silver_schema():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")

        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)

        con.execute("USE movilidad;")

        con.execute("""
            CREATE SCHEMA IF NOT EXISTS silver;
        """)

        con.close()

    schema = setup_silver_schema()

    @task(
        retries=3,
        retry_delay=timedelta(minutes=1),
        max_active_tis_per_dag=1
    )
    def create_silver_provincias():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")

        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)

        con.execute("USE movilidad;")
        con.execute("CREATE SCHEMA IF NOT EXISTS silver;")

        con.execute("""
            DROP TABLE IF EXISTS silver.provincias;

            CREATE TABLE silver.provincias (
                cod_provincia       INTEGER NOT NULL,
                nombre_provincia    VARCHAR NOT NULL,
                nombre_alternativo  VARCHAR NOT NULL,
                cod_ccaa            INTEGER NOT NULL,
                nombre_ccaa         VARCHAR NOT NULL,
                geom                GEOMETRY NOT NULL
            );
        """)

        con.close()

    @task(
    retries=3,
    retry_delay=timedelta(minutes=1),
    max_active_tis_per_dag=1)
    def load_silver_provincias():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")

        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)

        con.execute("USE movilidad;")

        con.execute("""
            INSERT INTO silver.provincias
            SELECT
                CAST(Codigo AS INTEGER)          AS cod_provincia,
                TRIM(CAST(Texto AS VARCHAR))     AS nombre_provincia,
                TRIM(CAST(Texto_Alt AS VARCHAR)) AS nombre_alternativo,
                CAST(Cod_CCAA AS INTEGER)        AS cod_ccaa,
                TRIM(CAST(CCAA AS VARCHAR))      AS nombre_ccaa,
                geom
            FROM bronze.provincias
            WHERE Codigo IS NOT NULL;
        """)

        con.close()

    create_provincias = create_silver_provincias()
    load_provincias = load_silver_provincias()


    @task
    def create_poblacion_table():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")

        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)
        con.execute("USE movilidad;")

        con.execute("""
            DROP TABLE IF EXISTS silver.poblacion;
        """)

        con.execute("""
            CREATE TABLE silver.poblacion (
                id INTEGER NOT NULL,
                poblacion INTEGER NOT NULL
            );
        """)

        con.close()

    @task
    def populate_poblacion_table():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")

        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)
        con.execute("USE movilidad;")

        con.execute("""
            INSERT INTO silver.poblacion
            WITH pob_raw AS (
                SELECT
                    "column0" AS col_codigo,
                    "Total" AS col_pob
                FROM bronze.poblacion
            ),
            limpio AS (
                SELECT
                    SUBSTR(TRIM(col_codigo), 1, 5) AS codigo_raw,
                    TRIM(col_pob) AS pob_raw
                FROM pob_raw
                WHERE TRIM(col_codigo) <> '2023'
            )
            SELECT
                CAST(codigo_raw AS INTEGER) AS id,
                CAST(REPLACE(pob_raw, ',', '') AS INTEGER) AS poblacion
            FROM limpio
            WHERE
                TRY_CAST(codigo_raw AS INTEGER) IS NOT NULL
                AND pob_raw ~ '.*[0-9].*';
        """)

        con.close()

    create_poblacion = create_poblacion_table()
    populate_poblacion = populate_poblacion_table()

    create_poblacion >> populate_poblacion

    @task
    def create_renta_table():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")

        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)
        con.execute("USE movilidad;")

        con.execute("""
            DROP TABLE IF EXISTS silver.renta;
        """)

        con.execute("""
            CREATE TABLE silver.renta (
                id INTEGER NOT NULL,
                renta INTEGER NOT NULL
            );
        """)
        con.close()

    @task
    def populate_renta_table():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")

        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)
        con.execute("USE movilidad;")

        con.execute("""
            INSERT INTO silver.renta
            WITH renta_raw AS (
                SELECT
                    column0 AS col_codigo,
                    "Renta neta media por persona" AS col_renta
                FROM bronze.renta
            ),
            limpio AS (
                SELECT
                    SUBSTR(TRIM(col_codigo), 1, 5) AS codigo_raw,
                    TRIM(col_renta) AS renta_raw
                FROM renta_raw
                WHERE TRIM(col_codigo) <> '2023'
            )
            SELECT
                CAST(codigo_raw AS INTEGER) AS id,
                CAST(REPLACE(renta_raw, ',', '') AS INTEGER) AS renta
            FROM limpio
            WHERE
                TRY_CAST(codigo_raw AS INTEGER) IS NOT NULL
                AND renta_raw ~ '.*[0-9].*';
        """)

        con.close()
    create_renta = create_renta_table()
    populate_renta = populate_renta_table()
    create_renta >> populate_renta

    @task
    def create_calendario_table():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")

        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)
        con.execute("USE movilidad;")

        con.execute("""
            DROP TABLE IF EXISTS silver.calendario;
        """)

        con.execute("""
            CREATE TABLE silver.calendario (
                fecha DATE NOT NULL,
                dia_semana VARCHAR NOT NULL,
                tipo_dia BOOLEAN NOT NULL
            );
        """)

        con.close()

    @task
    def populate_calendario_table():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")

        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)
        con.execute("USE movilidad;")

        con.execute("""
            INSERT INTO silver.calendario
            SELECT
                CAST(Dia AS DATE) AS fecha,
                LOWER(TRIM(Dia_semana)) AS dia_semana,
                CAST("laborable" AS BOOLEAN) AS tipo_dia
            FROM bronze.calendario
            WHERE
                Dia IS NOT NULL
                AND Dia_semana IS NOT NULL
                AND "laborable" IS NOT NULL;
        """)

        con.close()
    create_cal = create_calendario_table()
    populate_cal = populate_calendario_table()
    create_cal >> populate_cal

    @task
    def create_viajes_table():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")

        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)
        con.execute("USE movilidad;")

        con.execute("""
            DROP TABLE IF EXISTS silver.viajes;
        """)

        con.execute("""
            CREATE TABLE silver.viajes (
                fecha DATE NOT NULL,
                periodo INTEGER NOT NULL,
                tipo_zona VARCHAR NOT NULL,
                id_origen INTEGER NOT NULL,
                id_destino INTEGER NOT NULL,
                viajes INTEGER NOT NULL,
                viajes_metros DOUBLE NOT NULL
            );
        """)

        con.close() 


    def get_year_2023_files():
        import pandas as pd
        dr = pd.date_range(start='2023-01-01', end='2023-12-31')
        
        # Definimos los tipos y sus sufijos correspondientes en S3
        configuraciones = [
            {"tipo": "municipios", "sufijo": "Viajes_municipios"},
            {"tipo": "distritos", "sufijo": "Viajes_distritos"},
            {"tipo": "gaus", "sufijo": "Viajes_GAU"}
        ]
        
        lista_archivos = []
        for conf in configuraciones:
            for d in dr:
                dia_str = d.strftime('%Y%m%d')
                mes_str = d.strftime('%m')
                path = f"s3://mobility-shared/prebronze/viajes/{conf['tipo']}/2023/{mes_str}/{dia_str}_{conf['sufijo']}.csv.gz"
                
                lista_archivos.append({
                    "dia": dia_str,
                    "path": path,
                    "tipo": conf['tipo']
                })
        return lista_archivos

    @task(
        max_active_tis_per_dag=1, 
        retries=1
    )
    def populate_viajes_table(config: dict): 
        dia = config['dia']
        path = config['path']
        tipo = config['tipo']
        fecha_iso = f"{dia[0:4]}-{dia[4:6]}-{dia[6:8]}"
        
        print(f"🚀 Procesando archivo directo: {path}")
        
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")
        con = init_duckdb(aws_conn, pg_conn)

        # --- OPTIMIZACIÓN DE MOTOR (Para 32GB RAM) ---
        con.execute("SET memory_limit='20GB';") # Reservamos 20GB para DuckDB
        con.execute("SET temp_directory='/tmp/duckdb_temp';") # Spill-to-disk preventivo
        con.execute("SET preserve_insertion_order=false;") # Aumenta velocidad de INSERT

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)
        con.execute("USE movilidad;")

        # Definimos qué columnas del maestro usar según el nivel actual
        mapping_cols = {
            'municipios': ('municipio_mitma', 'municipio_ine'),
            'distritos': ('distrito_mitma', 'distrito_ine'),
            'gaus': ('gau_mitma', 'distrito_ine')
        }
        mitma_col, ine_col = mapping_cols[tipo]

        # Borramos datos previos del mismo día y tipo para mantener la idempotencia
        con.execute(f"DELETE FROM silver.viajes WHERE fecha = '{fecha_iso}' AND tipo_zona = '{tipo}';")

        print(f"📥 Insertando datos transformando {mitma_col} a {ine_col}...")
        
        # La magia ocurre aquí: leemos el CSV como texto para que '01004_AM' no falle,
        # y lo unimos con la tabla relaciones para obtener el ID INE numérico
        con.execute(f"""
            INSERT INTO silver.viajes (fecha, periodo, tipo_zona, id_origen, id_destino, viajes, viajes_metros)
            SELECT  
                CAST('{fecha_iso}' AS DATE),
                CAST(v.periodo AS INTEGER),
                '{tipo}',
                TRY_CAST(r_orig.{ine_col} AS INTEGER), 
                TRY_CAST(r_dest.{ine_col} AS INTEGER),
                CAST(v.viajes AS INTEGER),
                CAST(TRY_CAST(v.viajes_km AS DOUBLE) * 1000 AS DOUBLE)
            FROM read_csv('{path}', ALL_VARCHAR=TRUE) v
            INNER JOIN (SELECT DISTINCT {mitma_col}, {ine_col} FROM bronze.relaciones) r_orig 
                ON v.origen = r_orig.{mitma_col}
            INNER JOIN (SELECT DISTINCT {mitma_col}, {ine_col} FROM bronze.relaciones) r_dest 
                ON v.destino = r_dest.{mitma_col}
            -- Filtro para asegurar que no insertamos NULLs procedentes de los 'NA'
            WHERE v.viajes_km IS NOT NULL
            AND TRY_CAST(r_orig.{ine_col} AS INTEGER) IS NOT NULL
            AND TRY_CAST(r_dest.{ine_col} AS INTEGER) IS NOT NULL;
        """)

        con.close()
        print(f"✅ Finalizada carga de {tipo} para {fecha_iso}")
        
        con.close()

    create_viajes = create_viajes_table()
    
    config_anual = get_year_2023_files() 
    
    populate_viajes = populate_viajes_table.expand(config=config_anual)

    # Provincias
    schema>>provincias >> create_provincias >> load_provincias

    # Poblacion
    schema>>poblacion >> create_poblacion >> populate_poblacion

    # Renta
    schema >> renta >> create_renta >> populate_renta

    # Calendario
    schema >> calendario >> create_cal >> populate_cal

    # Viajes    
    schema >> [insert_municipios, insert_distritos, insert_gaus, relaciones] >> create_viajes >> populate_viajes
    
    @task
    def create_lugares_table():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")

        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)
        con.execute("USE movilidad;")

        con.execute("""
            DROP TABLE IF EXISTS silver.lugares;
        """)

        con.execute("""
            CREATE TABLE silver.lugares (
                id INTEGER NOT NULL,
                id_mitma VARCHAR NOT NULL,
                nombre VARCHAR NOT NULL,
                coordenadas GEOMETRY NOT NULL,
                tipo_zona VARCHAR NOT NULL,
                longitude DOUBLE NOT NULL,
                latitude DOUBLE NOT NULL,
                cod_provincia INTEGER
            );
        """)

        con.close()
    
    @task
    def populate_lugares_table():
        aws_conn = BaseHook.get_connection("aws_s3_conn")
        pg_conn = BaseHook.get_connection("my_postgres_conn")

        con = init_duckdb(aws_conn, pg_conn)

        con.execute("""
            ATTACH 'ducklake:secreto_ducklake' AS movilidad (
                DATA_PATH 's3://mobility-shared-2/',
                OVERRIDE_DATA_PATH TRUE
            );
        """)
        con.execute("USE movilidad;")

        con.execute("""
        INSERT INTO silver.lugares
        WITH geo_unificada AS (
            -- Municipios
            SELECT DISTINCT
                TRY_CAST(r.municipio_ine AS INTEGER) AS id, -- Convierte 'NA' en NULL
                m.id AS id_mitma,
                TRIM(m.name) AS nombre, 
                m.geom AS coordenadas, 
                'municipio' AS tipo_zona
            FROM bronze.geo_municipios m
            JOIN bronze.relaciones r ON m.id = r.municipio_mitma
            
            UNION ALL
            
            -- Distritos
            SELECT DISTINCT
                TRY_CAST(r.distrito_ine AS INTEGER) AS id,
                m.id AS id_mitma,
                TRIM(m.name), 
                m.geom, 
                'distrito'
            FROM bronze.geo_distritos m
            JOIN bronze.relaciones r ON m.id = r.distrito_mitma
            
            UNION ALL
            
            -- GAUs
            SELECT DISTINCT
                TRY_CAST(r.distrito_ine AS INTEGER) AS id,
                m.id AS id_mitma,
                TRIM(m.name), 
                m.geom, 
                'gau'
            FROM bronze.geo_gaus m
            JOIN bronze.relaciones r ON m.id = r.gau_mitma
        )
        SELECT
            g.id, g.id_mitma, g.nombre, g.coordenadas, g.tipo_zona,
            ST_X(ST_Centroid(g.coordenadas)) AS longitude,
            ST_Y(ST_Centroid(g.coordenadas)) AS latitude,
            p.cod_provincia
        FROM geo_unificada g
        LEFT JOIN silver.provincias p ON ST_Contains(p.geom, ST_Centroid(g.coordenadas))
        -- Limpieza: descartamos los 'NA' (NULLs) y nombres vacíos
        WHERE g.id IS NOT NULL 
        AND g.nombre IS NOT NULL;
        """)
        con.close()

    create_lugares = create_lugares_table()
    populate_lugares = populate_lugares_table()

    schema >> [geo_municipios, geo_distritos, geo_gaus] >> create_lugares >> populate_lugares


dag = mobility_bronze_silver()

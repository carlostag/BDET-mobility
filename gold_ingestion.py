from airflow.sdk import dag, task
from pendulum import datetime
from airflow.sdk.bases.hook import BaseHook

import duckdb


# =====================================================
# DuckDB + DuckLake connection
# =====================================================
def get_con_and_attach():
    aws = BaseHook.get_connection("aws_s3_conn")
    pg = BaseHook.get_connection("my_postgres_conn")

    con = duckdb.connect(database=":memory:")

    con.execute("""
        INSTALL ducklake;
        INSTALL spatial;
        INSTALL postgres;
        LOAD ducklake;
        LOAD spatial;
    """)

    con.execute("""
        SET max_memory='24GB';
        SET threads=10;
        SET ducklake_max_retry_count = 100;
    """)

    con.execute(f"""
        SET s3_region='{aws.extra_dejson.get("region", "eu-central-1")}';
        SET s3_url_style='path';
        SET s3_access_key_id='{aws.login}';
        SET s3_secret_access_key='{aws.password}';
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
    """)

    con.execute("""
        CREATE OR REPLACE SECRET secreto_ducklake (
            TYPE ducklake,
            METADATA_PATH '',
            METADATA_PARAMETERS MAP {
                'TYPE':'postgres',
                'SECRET':'secreto_postgres'
            }
        );
    """)

    con.execute("""
        ATTACH 'ducklake:secreto_ducklake' AS movilidad (
            DATA_PATH 's3://mobility-shared-2/',
            OVERRIDE_DATA_PATH TRUE
        );
    """)

    con.execute("USE movilidad;")
    return con


# =====================================================
# DAG definition
# =====================================================
@dag(
    dag_id="gold_mobility_by_tipo_zona_daily",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gold", "mobility"]
)

def gold_mobility_by_tipo_zona_daily_dag():

    # ----------------------------------------------
    # Configuración de tipos de zona
    # ----------------------------------------------
    ZONAS = {
        #"municipios": "gold.mobility_municipios_periodo",
        #"distritos":  "gold.mobility_distritos_periodo",
        "gaus":       "gold.mobility_gaus_periodo",
    }

    # ----------------------------------------------
    # 1. Crear tablas Gold
    # ----------------------------------------------
    @task
    def create_gold_tables():
        con = get_con_and_attach()

        for table_name in ZONAS.values():
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id_origen INTEGER NOT NULL,
                    id_destino INTEGER NOT NULL,
                    fecha DATE NOT NULL,
                    periodo VARCHAR NOT NULL,
                    total_viajes DOUBLE NOT NULL,
                    avg_viajes_metros DOUBLE NOT NULL
                );
            """)

        con.close()

    creacion = create_gold_tables()

    # ----------------------------------------------
    # 2. Generar lista de días
    # ----------------------------------------------
    @task
    def list_days_2023():
        from datetime import date, timedelta

        start = date(2023, 1, 1)
        end = date(2023, 12, 31)

        days = []
        d = start
        while d <= end:
            days.append(d.isoformat())
            d += timedelta(days=1)

        return days


    # ----------------------------------------------
    # 3. Procesar un día y un tipo de zona
    # ----------------------------------------------
    @task(max_active_tis_per_dag=1, retries=5)
    def process_one_day_one_zone(day: str, tipo_zona: str, table_name: str):
        con = get_con_and_attach()

        # Idempotencia
        con.execute(f"""
            DELETE FROM {table_name}
            WHERE fecha = DATE '{day}';
        """)

        # Inserción agregada
        con.execute(f"""
            INSERT INTO {table_name}
            SELECT
                id_origen,
                id_destino,
                fecha,
                periodo,
                SUM(viajes)        AS total_viajes,
                AVG(viajes_metros) AS avg_viajes_metros
            FROM silver.viajes
            WHERE
                fecha = DATE '{day}'
                AND tipo_zona = '{tipo_zona}'
            GROUP BY
                id_origen,
                id_destino,
                fecha,
                periodo;
        """)

        con.close()

    # ----------------------------------------------
    # 4. Orquestación
    # ----------------------------------------------
    days = list_days_2023()


    for tipo_zona, table_name in ZONAS.items():
        creacion >> process_one_day_one_zone.expand(
            day=days,
            tipo_zona=[tipo_zona],
            table_name=[table_name]
        )


dag = gold_mobility_by_tipo_zona_daily_dag()

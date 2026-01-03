from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from pendulum import datetime
import duckdb

# ===============================================================
# 1. CONFIGURACIÓN Y UTILIDADES
# ===============================================================

def init_duckdb(aws_conn, pg_conn):
	"""Inicializa DuckDB local con las extensiones y secretos necesarios."""
	con = duckdb.connect(database=":memory:")
	con.execute("INSTALL ducklake; INSTALL postgres; INSTALL spatial; INSTALL httpfs;")
	con.execute("LOAD ducklake; LOAD spatial; LOAD postgres; LOAD httpfs;")
	con.execute(f"""
		SET s3_region           = 'eu-central-1'; 
		SET s3_access_key_id     = '{aws_conn.login}'; 
		SET s3_secret_access_key = '{aws_conn.password}';
	""")
	con.execute(f"""
		CREATE OR REPLACE SECRET secreto_postgres (
			TYPE POSTGRES, HOST '{pg_conn.host}', PORT {pg_conn.port}, 
			DATABASE '{pg_conn.schema}', USER '{pg_conn.login}', PASSWORD '{pg_conn.password}'
		);
	""")
	con.execute("CREATE OR REPLACE SECRET secreto_ducklake (TYPE DUCKLAKE, METADATA_PATH '', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'secreto_postgres'});")
	return con

def get_2023_week_paths():
	"""Genera la lista de los primeros 7 días de enero de 2023."""
	base = "s3://dl-mobility-spain/audit/viajes/municipios/2023"
	suffix = "Viajes_municipios"
	return [f"{base}/01/202301{day:02d}_{suffix}.csv.gz" for day in range(1, 8)]

# ===============================================================
# 2. DEFINICIÓN DEL DAG
# ===============================================================

@dag(
	dag_id="mobility_ONE_WEEK_TEST_2023",
	start_date=datetime(2025, 1, 1),
	schedule=None,
	catchup=False,
	tags=["test", "duckrunner", "production_ready"]
)
def mobility_ingestion():

	# --- BRONZE: Ingestión de Dimensiones (INE/MITMA) ---
	@task
	def ingest_bronze_official_data():
		aws_conn = BaseHook.get_connection("aws_s3_conn")
		pg_conn = BaseHook.get_connection("my_postgres_conn")
		con = init_duckdb(aws_conn, pg_conn)
		con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS lake (DATA_PATH 's3://pruebas-airflow-carlos/', OVERRIDE_DATA_PATH TRUE);")
		con.execute("CREATE SCHEMA IF NOT EXISTS lake.bronze;")
		
		con.execute("CREATE OR REPLACE TABLE lake.bronze.renta AS SELECT * FROM read_csv_auto('https://www.ine.es/jaxiT3/files/t/es/csv_bd/30824.csv?nocab=1', sep='\\t', header=true, all_varchar=true);")
		con.execute("CREATE OR REPLACE TABLE lake.bronze.poblacion AS SELECT * FROM read_csv_auto('https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/poblacion_municipios.csv', sep='|', header=false, all_varchar=true);")
		con.execute("CREATE OR REPLACE TABLE lake.bronze.geo_municipios AS SELECT * FROM ST_Read('https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/zonificacion_municipios.shp');")
		con.execute("CREATE OR REPLACE TABLE lake.bronze.relaciones AS SELECT * FROM read_csv_auto('https://movilidad-opendata.mitma.es/zonificacion/relacion_ine_zonificacionMitma.csv', sep='|', header=true, all_varchar=true);")
		con.close()

	# --- BRONZE: Inicialización Tabla de Hechos ---
	@task
	def create_viajes_bronze_table_init():
		aws_conn = BaseHook.get_connection("aws_s3_conn")
		pg_conn = BaseHook.get_connection("my_postgres_conn")
		con = init_duckdb(aws_conn, pg_conn)
		con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS lake (DATA_PATH 's3://pruebas-airflow-carlos/', OVERRIDE_DATA_PATH TRUE);")
		con.execute("CREATE OR REPLACE TABLE lake.bronze.trips_municipios_2023 AS SELECT * FROM read_csv_auto('s3://dl-mobility-spain/audit/viajes/municipios/2023/01/20230101_Viajes_municipios.csv.gz', all_varchar=true) LIMIT 0;")
		con.close()

	# --- BRONZE: Carga Batch (Inyección Exhaustiva de Variables) ---
	aws_conn = BaseHook.get_connection("aws_s3_conn")
	pg_conn = BaseHook.get_connection("my_postgres_conn")

	

	batch_overrides_list = [
		{
			"environment": [
				# --- POSTGRES (Todas las variantes posibles) ---
				{"name": "HOST_POSTGRES",      "value": pg_conn.host},
				{"name": "IP_POSTGRES",        "value": pg_conn.host},
				{"name": "PUERTO_POSTGRES",    "value": str(pg_conn.port)},
				{"name": "PORT_POSTGRES",      "value": str(pg_conn.port)},
				{"name": "USUARIO_POSTGRES",   "value": pg_conn.login},
				{"name": "USER_POSTGRES",      "value": pg_conn.login},
				{"name": "PASS_POSTGRES",      "value": pg_conn.password},
				{"name": "PASSWORD_POSTGRES",  "value": pg_conn.password},
				{"name": "CLAVE_POSTGRES",     "value": pg_conn.password},
				{"name": "CONTRA_POSTGRES",    "value": pg_conn.password},
				{"name": "DB_POSTGRES",        "value": pg_conn.schema},
				{"name": "DATABASE_POSTGRES",  "value": pg_conn.schema},
				{"name": "BASE_DATOS_POSTGRES","value": pg_conn.schema},
				{"name": "ESQUEMA_POSTGRES",   "value": "public"},
				{"name": "SCHEMA_POSTGRES",    "value": "public"},
				
				# --- AWS / S3 ---
				{"name": "AWS_ACCESS_KEY_ID",     "value": aws_conn.login},
				{"name": "AWS_SECRET_ACCESS_KEY", "value": aws_conn.password},
				{"name": "AWS_REGION",            "value": "eu-central-1"},
				{"name": "REGION_AWS",            "value": "eu-central-1"},
				{"name": "DATA_PATH",             "value": "s3://pruebas-airflow-carlos/"},
				{"name": "S3_PATH",               "value": "s3://pruebas-airflow-carlos/"},
				{"name": "BUCKET_S3",             "value": "pruebas-airflow-carlos"},
				
				# --- EJECUCIÓN ---
				{
					"name": "SQL_QUERY", 
					"value": f"INSERT INTO lake.bronze.trips_municipios_2023 SELECT * FROM read_csv('{fp}', header=true, auto_detect=true, all_varchar=true);"
				}
			]
		}
		for fp in get_2023_week_paths()
	]

	load_viajes_batch = BatchOperator.partial(
		task_id         = "load_viajes_batch",
		job_name        = "ingest_trip_week_test",
		job_queue       = "DuckJobQueue",
		job_definition  = "EC2JobDefinition:2", 
		region_name     = "eu-central-1"
	).expand(container_overrides=batch_overrides_list)

	# --- SILVER: Transformación con Tabs Limpios ---
	@task
	def create_silver_layer():
		aws_conn = BaseHook.get_connection("aws_s3_conn")
		pg_conn = BaseHook.get_connection("my_postgres_conn")
		con = init_duckdb(aws_conn, pg_conn)
		con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS lake (DATA_PATH 's3://pruebas-airflow-carlos/', OVERRIDE_DATA_PATH TRUE);")
		con.execute("CREATE SCHEMA IF NOT EXISTS lake.silver; USE lake;")

		# Transformación Silver Población
		con.execute("""
			CREATE OR REPLACE TABLE silver.poblacion AS 
			SELECT 
				CAST(SUBSTR(TRIM(column0), 1, 5) AS INTEGER) AS id, 
				CAST(REPLACE(column1, ',', '') AS INTEGER)  AS poblacion 
			FROM bronze.poblacion 
			WHERE TRY_CAST(REPLACE(column1, ',', '') AS INTEGER) IS NOT NULL;
		""")

		# Transformación Silver Renta
		con.execute("""
			CREATE OR REPLACE TABLE silver.renta AS 
			SELECT 
				CAST(SUBSTR(TRIM(column0), 1, 5) AS INTEGER)                      AS id, 
				CAST(REPLACE("Renta neta media por persona", '.', '') AS INTEGER) AS renta 
			FROM bronze.renta 
			WHERE Periodo = '2023';
		""")

		# Transformación Silver Lugares (Dimensiones Espaciales)
		con.execute("""
			CREATE OR REPLACE TABLE silver.lugares AS 
			SELECT 
				CAST(r.municipio AS INTEGER)    AS id, 
				r.municipio_mitma               AS id_mitma, 
				g.NAME                          AS nombre, 
				g.geom                          AS coordenadas, 
				'municipio'                     AS tipo_zona, 
				ST_X(ST_Centroid(g.geom))       AS longitude, 
				ST_Y(ST_Centroid(g.geom))       AS latitude 
			FROM bronze.relaciones r 
			JOIN bronze.geo_municipios g 
				ON TRIM(r.municipio_mitma) = TRIM(g.ID);
		""")

		# Transformación Silver Viajes (Hechos transformados)
		con.execute("""
			CREATE OR REPLACE TABLE silver.viajes AS 
			SELECT 
				CAST(STRPTIME(CAST(fecha AS VARCHAR), '%Y%m%d') AS DATE) AS fecha, 
				CAST(periodo AS INTEGER)         AS periodo, 
				CAST(origen AS INTEGER)          AS id_origen, 
				CAST(destino AS INTEGER)         AS id_destino, 
				CAST(viajes AS INTEGER)          AS viajes, 
				CAST(viajes_km * 1000 AS DOUBLE) AS viajes_metros 
			FROM bronze.trips_municipios_2023 
			WHERE viajes_km IS NOT NULL;
		""")
		con.close()

	# --- FLUJO DE TRABAJO ---
	[ingest_bronze_official_data(), create_viajes_bronze_table_init()] >> load_viajes_batch >> create_silver_layer()

mobility_dag = mobility_ingestion()

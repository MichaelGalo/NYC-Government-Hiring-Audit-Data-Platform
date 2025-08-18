import os
import time
import sys
import duckdb
current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)
from src.logger import setup_logging
from dotenv import load_dotenv
load_dotenv()

logger = setup_logging()

total_start_time = time.time()
logger.info("Starting NYC Jobs Audit data pipeline")

logger.info("Installing and loading DuckDB extensions")
duckdb.install_extension("ducklake")
duckdb.install_extension("httpfs")
duckdb.load_extension("ducklake")
duckdb.load_extension("httpfs")
logger.info("DuckDB extensions loaded successfully")

# Explicitly define database and data paths
db_path = os.path.join(parent_path, "nyc_jobs_audit.db")
con = duckdb.connect(db_path)
logger.info(f"Connected to persistent DuckDB database: {db_path}")

data_path = os.path.join(parent_path, "data")
catalog_path = os.path.join(parent_path, "catalog.ducklake")

logger.info(f"Attaching DuckLake with data path: {data_path}")
con.execute(f"ATTACH 'ducklake:{catalog_path}' AS my_ducklake (DATA_PATH '{data_path}')")
con.execute("USE my_ducklake")
logger.info("DuckLake attached and activated successfully")

# Configure MinIO settings
logger.info("Configuring MinIO S3 settings")
con.execute(f"SET s3_access_key_id = '{os.getenv('MINIO_ACCESS_KEY')}'")
con.execute(f"SET s3_secret_access_key = '{os.getenv('MINIO_SECRET_KEY')}'")
con.execute(f"SET s3_endpoint = '{os.getenv('MINIO_EXTERNAL_URL')}'")
con.execute("SET s3_use_ssl = false")
con.execute("SET s3_url_style = 'path'")
logger.info("MinIO S3 configuration completed")

# Define Schemas
logger.info("Creating database schemas")
con.execute("CREATE SCHEMA IF NOT EXISTS BRONZE")
con.execute("CREATE SCHEMA IF NOT EXISTS GOLD")
logger.info("Database schemas created successfully")

# Run Bronze Ingestion
bronze_start_time = time.time()
logger.info("Starting Bronze layer ingestion")
bucket_name = os.getenv('MINIO_BUCKET_NAME')
file_list_query = f"SELECT * FROM glob('s3://{bucket_name}/*.csv')"

try:
    files_result = con.execute(file_list_query).fetchall()
    file_paths = []
    for row in files_result:
        file_paths.append(row[0])
    
    logger.info(f"Found {len(file_paths)} files in MinIO bucket")
    
    for file_path in file_paths:
        file_name = os.path.basename(file_path).replace('.csv', '')
        table_name = file_name.lower().replace('-', '_').replace(' ', '_')

        logger.info(f"Processing file: {file_path} -> table: BRONZE.{table_name}_raw")

        BRONZE_query = f"""
        CREATE TABLE IF NOT EXISTS BRONZE.{table_name}_raw AS
        SELECT 
            *,
            '{file_name}' AS _source_file,
            CURRENT_TIMESTAMP AS _ingestion_timestamp,
            ROW_NUMBER() OVER () AS _record_id
        FROM read_csv_auto('{file_path}', header=true, ignore_errors=true, all_varchar=true);
        """
        
        con.execute(BRONZE_query)
        logger.info(f"Successfully created BRONZE.{table_name}_raw")

except Exception as e:
    logger.error(f"Error processing files from MinIO: {e}")
    raise

bronze_end_time = time.time()
logger.info(f"Bronze layer ingestion completed in {bronze_end_time - bronze_start_time:.2f} seconds")

con.close()
logger.info("Database connection closed")

total_end_time = time.time()
logger.info(f"Database Init & Bronze Ingestion Layer completed in {total_end_time - total_start_time:.2f} seconds")

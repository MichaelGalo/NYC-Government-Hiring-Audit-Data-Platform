from logger import setup_logging
import os
import sys
import time
import duckdb
from prefect import task
from utils import update_data
from dotenv import load_dotenv
current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)
load_dotenv()

logger = setup_logging()

@task(name="database_synchronization")
def db_sync():
    total_start_time = time.time()
    logger.info("Starting NYC Jobs Audit data pipeline")

    logger.info("Installing and loading DuckDB extensions")
    duckdb.install_extension("ducklake")
    duckdb.install_extension("httpfs")
    duckdb.load_extension("ducklake")
    duckdb.load_extension("httpfs")
    logger.info("DuckDB extensions loaded successfully")

    db_path = os.path.join(parent_path, "nyc_jobs_audit.db")
    con = duckdb.connect(db_path)
    logger.info(f"Connected to persistent DuckDB database: {db_path}")

    data_path = os.path.join(parent_path, "data")
    catalog_path = os.path.join(parent_path, "catalog.ducklake")

    logger.info(f"Attaching DuckLake with data path: {data_path}")
    con.execute(f"ATTACH 'ducklake:{catalog_path}' AS my_ducklake (DATA_PATH '{data_path}')")
    con.execute("USE my_ducklake")
    logger.info("DuckLake attached and activated successfully")

    logger.info("Configuring MinIO S3 settings")
    con.execute(f"SET s3_access_key_id = '{os.getenv('MINIO_ACCESS_KEY')}'")
    con.execute(f"SET s3_secret_access_key = '{os.getenv('MINIO_SECRET_KEY')}'")
    con.execute(f"SET s3_endpoint = '{os.getenv('MINIO_EXTERNAL_URL')}'")
    con.execute("SET s3_use_ssl = false")
    con.execute("SET s3_url_style = 'path'")
    logger.info("MinIO S3 configuration completed")

    logger.info("Creating database schemas")
    con.execute("CREATE SCHEMA IF NOT EXISTS BRONZE")
    con.execute("CREATE SCHEMA IF NOT EXISTS GOLD")
    logger.info("Database schemas created successfully")

    bronze_start_time = time.time()
    logger.info("Starting Bronze layer ingestion")
    minio_bucket = os.getenv('MINIO_BUCKET_NAME')

    # creates initial database & also refreshes on new data ingestion
    update_data(con, logger, minio_bucket)

    bronze_end_time = time.time()
    logger.info(f"Bronze layer ingestion completed in {bronze_end_time - bronze_start_time:.2f} seconds")

    con.close()
    logger.info("Database connection closed")

    total_end_time = time.time()
    logger.info(f"Database Init & Bronze Ingestion Layer completed in {total_end_time - total_start_time:.2f} seconds")
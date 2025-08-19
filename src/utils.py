import unicodedata
from logger import setup_logging
from dotenv import load_dotenv
import os
import io
import glob
from minio import Minio
import duckdb
import sys
current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)

load_dotenv()
logger = setup_logging() 

def normalize_string(input_string):
    # normalizes for string comparison
    if input_string is None:
        return ""
    normalized = unicodedata.normalize("NFKC", input_string).strip().upper()
    return normalized

def get_latest_file(directory, extension="*.parquet"):
    files = glob.glob(os.path.join(directory, extension))
    if not files:
        raise FileNotFoundError(f"No files with extension {extension} found in directory: {directory}")
    latest_file = max(files, key=os.path.getctime)
    return latest_file

def write_csv_to_minio_stream(df, object_name):
    client = Minio(
        endpoint=os.getenv("MINIO_EXTERNAL_URL"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )
    minio_bucket = os.getenv("MINIO_BUCKET_NAME")
    try:
        buffer = io.BytesIO()
        df.write_csv(buffer)
        buffer.seek(0)
        client.put_object(
            minio_bucket,
            object_name,
            buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/csv"
        )
        logger.info(f"Streamed CSV to minio://{minio_bucket}/{object_name}")
    except Exception as e:
        logger.error(f"Error streaming to MinIO: {e}")


def write_dataframe_to_bronze_table(df, table_name):
    duckdb.install_extension("ducklake")
    duckdb.install_extension("httpfs")
    duckdb.load_extension("ducklake")
    duckdb.load_extension("httpfs")
    db_path = os.path.join(parent_path, "nyc_jobs_audit.db")
    con = duckdb.connect(db_path)
    data_path = os.path.join(parent_path, "data")
    catalog_path = os.path.join(parent_path, "catalog.ducklake")
    con.execute(f"ATTACH 'ducklake:{catalog_path}' AS my_ducklake (DATA_PATH '{data_path}')")
    con.execute("USE my_ducklake")

    try:
        con.execute(f"CREATE TABLE IF NOT EXISTS BRONZE.{table_name}_raw AS SELECT * FROM df")
        logger.info(f"Successfully created BRONZE.{table_name}_raw")

    except Exception as e:
        logger.error(f"Error writing DataFrame to DuckDB: {e}")
        raise
    finally:
        con.close()
        logger.info("Closed DuckDB connection")

def update_data(con, logger, bucket_name):
    logger.info("Starting Bronze layer ingestion")
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
            logger.info(f"Successfully created or updated BRONZE.{table_name}_raw")

    except Exception as e:
        logger.error(f"Error processing files from MinIO: {e}")
        raise
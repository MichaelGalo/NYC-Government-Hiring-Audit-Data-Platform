from logger import setup_logging
from dotenv import load_dotenv
import os
import io
import string
import glob
import re
from minio import Minio
import sys
import polars as pl
current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)

load_dotenv()
logger = setup_logging() 


punctuation_table = str.maketrans("", "", string.punctuation)

def normalize_title(title):

    if not isinstance(title, str):
        return ""
    title = title.lower()
    title = title.translate(punctuation_table)
    title = re.sub(r"\s+", " ", title)
    return title.strip()


def get_most_recent_file(path, extension="*.parquet"):
    if os.path.isfile(path):
        return path
    matches = (
        glob.glob(path)
        or glob.glob(os.path.join(path, extension))
        or glob.glob(os.path.join(path, "**", extension), recursive=True)
    )
    if not matches:
        raise FileNotFoundError(f"No files found matching {path} (ext={extension})")
    return max(matches, key=os.path.getctime)

def chunked(iterable, size):
    total_length = len(iterable)
    for start_index in range(0, total_length, size):
        end_index = min(start_index + size, total_length)
        yield start_index, end_index, iterable[start_index:end_index]

def write_batch_to_parquet(output_buffer, output_schema, output_parquet, batch_count):
    for row in output_buffer:
        for date_col in ["posting_date", "post_until"]:
            if row[date_col] is not None:
                row[date_col] = str(row[date_col])
    batch_filename = output_parquet.replace(".parquet", f"_batch_{batch_count:03}.parquet")
    # allow output_schema to be optional; fall back to schema inference
    try:
        if output_schema is not None:
            pl.DataFrame(output_buffer, schema=output_schema).write_parquet(batch_filename)
        else:
            pl.DataFrame(output_buffer).write_parquet(batch_filename)
    except Exception:
        # final fallback: let polars infer schema and write
        pl.DataFrame(output_buffer).write_parquet(batch_filename)
    output_buffer.clear()
    return batch_count + 1

def merge_and_cleanup_batches(output_parquet, logger):
    batch_files_pattern = output_parquet.replace(".parquet", "_batch_*.parquet")
    batch_files = sorted(glob.glob(batch_files_pattern))
    if batch_files:
        logger.info(f"Merging {len(batch_files)} batch files into final Parquet...")
        merged_df = pl.concat([pl.read_parquet(f) for f in batch_files])
        merged_df.write_parquet(output_parquet)
        logger.info(f"Final Parquet written to {output_parquet}")
        for f in batch_files:
            os.remove(f)
        logger.info("Temporary batch files deleted.")
    else:
        logger.warning("No batch files found to merge.")

def upload_file_to_minio(file_path, bucket_name, object_name):
    client = Minio(
        os.getenv("MINIO_EXTERNAL_URL"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )
    with open(file_path, "rb") as fh:
        data = fh.read()
    client.put_object(
        bucket_name,
        object_name,
        io.BytesIO(data),
        length=len(data),
        content_type="application/x-parquet",
    )

def upload_parquet_and_remove_local(parquet_path, logger):
    bucket = os.getenv("MINIO_BUCKET_NAME")
    if not bucket:
        logger.warning("MINIO_BUCKET_NAME not set; skipping upload to MinIO")
        return False
    object_name = os.path.basename(parquet_path)
    try:
        upload_file_to_minio(parquet_path, bucket, object_name)
        logger.info(f"Uploaded {parquet_path} to MinIO://{bucket}/{object_name}")
        try:
            os.remove(parquet_path)
            logger.info(f"Removed local file {parquet_path} after upload.")
        except Exception as rm_err:
            logger.warning(f"Uploaded but failed to remove local file {parquet_path}: {rm_err}")
        return True
    except Exception as exc:
        logger.error(f"Failed to upload {parquet_path} to MinIO: {exc}")
        raise

def update_data(con, logger, bucket_name):
    logger.info("Starting Bronze layer ingestion")
    file_list_query = f"SELECT * FROM glob('s3://{bucket_name}/*.parquet')"

    try:
        files_result = con.execute(file_list_query).fetchall()
        file_paths = []
        for row in files_result:
            file_paths.append(row[0])
        
        logger.info(f"Found {len(file_paths)} files in MinIO bucket")
        
        for file_path in file_paths:
            file_name = os.path.basename(file_path).replace('.parquet', '')
            table_name = file_name.lower().replace('-', '_').replace(' ', '_')

            logger.info(f"Processing file: {file_path} -> table: BRONZE.{table_name}")

            BRONZE_query = f"""
            CREATE TABLE IF NOT EXISTS BRONZE.{table_name} AS
            SELECT 
                *,
                '{file_name}' AS _source_file,
                CURRENT_TIMESTAMP AS _ingestion_timestamp,
                ROW_NUMBER() OVER () AS _record_id
            FROM read_parquet('{file_path}');
            """
            
            con.execute(BRONZE_query)
            logger.info(f"Successfully created or updated BRONZE.{table_name}")

    except Exception as e:
        logger.error(f"Error processing files from MinIO: {e}")
        raise
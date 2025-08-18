import unicodedata
from logger import setup_logging
from dotenv import load_dotenv
import os
import io
import glob
from minio import Minio

load_dotenv()
logger = setup_logging() 

def normalize_string(input_string: str) -> str:
    # normalizes for string comparison
    if input_string is None:
        return ""
    normalized = unicodedata.normalize("NFKC", input_string).strip().upper()
    return normalized

def get_latest_file(directory, extension="*.parquet"):
    files = glob.glob(os.path.join(directory, extension))
    if not files:
        raise FileNotFoundError(f"No files with extension {extension} found in directory: {directory}")
    latest_file = max(files, key=os.path.getctime)  # Get the most recently created/modified file
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
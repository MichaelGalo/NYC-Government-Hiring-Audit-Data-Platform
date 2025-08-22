import os
import requests
from dotenv import load_dotenv
from minio import Minio
import io
import polars as pl
import time
from db_sync import db_sync
from logger import setup_logging
from prefect import flow, task
from prefect.client.schemas.schedules import CronSchedule
logger = setup_logging()
load_dotenv()

@task(name="Fetch API Data")
def fetch_api_data(base_url):
    limit = 50000
    offset = 0
    all_data = []

    batch = [None]  # considered falsy for break at `[]`
    total_records = 0
    while batch:
        paged_url = f"{base_url}?$limit={limit}&$offset={offset}"
        response = requests.get(paged_url)
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break
        all_data.extend(batch)
        total_records += len(batch)
        logger.info(f"Fetched batch starting at record {offset}, total records fetched: {total_records}")
        offset += limit

    api_data_dataframe = pl.DataFrame(all_data)
    return api_data_dataframe

@task(name="Convert CSV to Parquet")
def convert_csv_to_parquet(dataframe):
    buffer = io.BytesIO()
    try:
        dataframe.write_parquet(buffer)
        buffer.seek(0)
        result = buffer
        return result
    except Exception as e:
        logger.error(f"Failed to convert DataFrame to Parquet in-memory: {e}")
        return None

@task(name="Write Data to MinIO")
def write_data_to_minio(parquet_buffer, bucket_name, object_name):
    minio_client = Minio(
        os.getenv("MINIO_EXTERNAL_URL"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )
    parquet_buffer.seek(0)
    data_bytes = parquet_buffer.read()
    
    try:
        minio_client.put_object(
        bucket_name,
        object_name,
        io.BytesIO(data_bytes),
        length=len(data_bytes),
        content_type="application/x-parquet",
    )
    except Exception as e:
        logger.error(f"Failed to write data to MinIO: {e}")


@flow(name="Data_Ingestion_Flow")
def run_data_ingestion():
    tick = time.time()
    payroll_url = os.getenv("NYC_PAYROLL_DATA_API")
    job_postings_url = os.getenv("NYC_JOB_POSTINGS_API")
    minio_bucket = os.getenv("MINIO_BUCKET_NAME")
    nyc_payroll_filename = "nyc_payroll_data.parquet"
    nyc_job_postings_filename = "nyc_job_postings_data.parquet"

    nyc_payroll_dataframe = fetch_api_data(payroll_url)
    payroll_parquet_buffer = convert_csv_to_parquet(nyc_payroll_dataframe)
    logger.info("Writing NYC Payroll Data to MinIO Storage")
    write_data_to_minio(payroll_parquet_buffer, minio_bucket, nyc_payroll_filename)

    nyc_job_postings_dataframe = fetch_api_data(job_postings_url)
    job_postings_parquet_buffer = convert_csv_to_parquet(nyc_job_postings_dataframe)
    logger.info("Writing NYC Job Postings Data to MinIO Storage")
    write_data_to_minio(job_postings_parquet_buffer, minio_bucket, nyc_job_postings_filename)
    tock = time.time() - tick

    logger.info("Synchronizing Data to Database")
    db_sync()

    logger.info(f"Data ingestion completed in {tock:.2f} seconds.")

if __name__ == "__main__":
    run_data_ingestion.serve(
        name="Data_Ingestion",
        schedule=CronSchedule(
            cron="0 0 * * 0",
            timezone="UTC"
        ), # sundays at midnight
        tags=["data_ingestion", "weekly"]
    )
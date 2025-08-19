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

@task(name="Write Data to MinIO")
def write_data_to_minio(dataframe, bucket_name, object_name):
    minio_client = Minio(
        os.getenv("MINIO_EXTERNAL_URL"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )
    csv_buffer = io.BytesIO()
    dataframe.write_csv(csv_buffer)
    csv_buffer.seek(0)
    csv_bytes = csv_buffer.read()
    
    minio_client.put_object(
        bucket_name,
        object_name,
        io.BytesIO(csv_bytes),
        length=len(csv_bytes),
        content_type="text/csv",
    )


@flow(name="Data_Ingestion_Flow")
def run_data_ingestion():
    tick = time.time()
    payroll_url = os.getenv("NYC_PAYROLL_DATA_API")
    job_postings_url = os.getenv("NYC_JOB_POSTINGS_API")
    minio_bucket = os.getenv("MINIO_BUCKET_NAME")
    nyc_payroll_filename = "nyc_payroll_data.csv"
    nyc_job_postings_filename = "nyc_job_postings_data.csv"

    nyc_payroll_data = fetch_api_data(payroll_url)
    logger.info("Writing NYC Payroll Data to MinIO Storage")
    write_data_to_minio(nyc_payroll_data, minio_bucket, nyc_payroll_filename)

    nyc_job_postings_data = fetch_api_data(job_postings_url)
    logger.info("Writing NYC Job Postings Data to MinIO Storage")
    write_data_to_minio(nyc_job_postings_data, minio_bucket, nyc_job_postings_filename)
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
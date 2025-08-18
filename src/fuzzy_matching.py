from minio import Minio
import os
import io
import glob
import polars as pl
from rapidfuzz import fuzz, process
from dotenv import load_dotenv
from logger import setup_logging
import time
from prefect import task

logger = setup_logging()
load_dotenv()

def get_latest_file(directory, extension="*.parquet"):
    files = glob.glob(os.path.join(directory, extension))
    if not files:
        raise FileNotFoundError(f"No files with extension {extension} found in directory: {directory}")
    latest_file = max(files, key=os.path.getctime)  # Get the most recently created/modified file
    return latest_file


def calculate_fuzzy_match(target_string, candidate_list_of_strings):
    best_match = process.extractOne(target_string, candidate_list_of_strings, scorer=fuzz.token_set_ratio)
    if best_match:
        return best_match[0], best_match[1]
    return None, 0


def process_payroll_data(directory):
    payroll_file = get_latest_file(directory)
    logger.info(f"Using payroll data file: {payroll_file}")

    payroll_cols = [
        "title_description",
        "base_salary",
        "pay_basis",
        "regular_gross_paid",
        "total_ot_paid",
        "total_other_pay"
    ]
    payroll_data_lazy = pl.scan_parquet(payroll_file).select(payroll_cols)
    payroll_data_lazy = payroll_data_lazy.with_columns([
        pl.col("title_description").cast(pl.Utf8).alias("comparison_string")
    ])
    payroll_data_df = payroll_data_lazy.collect()
    return payroll_data_df


def load_and_prepare_job_postings(job_postings_file, job_posting_cols):
    logger.info("Scanning Job Postings File with only selected columns.")
    jobs_lazy = pl.scan_parquet(job_postings_file).select(job_posting_cols)
    jobs_lazy = jobs_lazy.with_columns([
        pl.col("posting_date").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.3f", strict=False).alias("posting_date"),
        pl.col("post_until").str.strptime(pl.Datetime, "%d-%b-%Y", strict=False).alias("post_until")
    ])
    jobs_lazy = jobs_lazy.with_columns([
        pl.when(pl.col("post_until").is_null())
        .then(pl.col("posting_date") + pl.duration(days=30))  # if there are nulls, default 30 days
        .otherwise(pl.col("post_until"))
        .alias("post_until")
    ])
    jobs_lazy = jobs_lazy.with_columns([
        (pl.col("post_until") - pl.col("posting_date")).dt.total_days().alias("posting_duration")
    ])
    return jobs_lazy.collect()


def match_job_posting_to_payroll(row, candidate_strings, payroll_lookup_df):
    target_string = row['business_title']
    match_str, match_ratio = calculate_fuzzy_match(target_string, candidate_strings)
    
    if match_ratio >= 85:

        payroll_row = payroll_lookup_df.filter(pl.col("comparison_string") == match_str)
        if payroll_row.height > 0:
            pr = payroll_row.row(0)
            actual_base_salary = float(pr[payroll_lookup_df.columns.index('base_salary')])
            posting_salary_min = float(row['salary_range_from']) if row['salary_range_from'] is not None else 0
            posting_salary_max = float(row['salary_range_to']) if row['salary_range_to'] is not None else 0

            # Salary overlap check
            if posting_salary_min <= actual_base_salary <= posting_salary_max:
                return {
                    "job_title": row['business_title'],
                    "match_ratio": match_ratio,
                    "posting_salary_range_from": posting_salary_min,
                    "posting_salary_range_to": posting_salary_max,
                    "actual_base_salary": actual_base_salary,
                    "posting_duration": row['posting_duration'],
                    "posting_date": row['posting_date'],
                    "posting_until": row['post_until'],
                    "actual_pay_basis": pr[payroll_lookup_df.columns.index('pay_basis')],
                    "actual_regular_gross_paid": pr[payroll_lookup_df.columns.index('regular_gross_paid')],
                    "actual_total_ot_paid": pr[payroll_lookup_df.columns.index('total_ot_paid')],
                    "actual_total_other_pay": pr[payroll_lookup_df.columns.index('total_other_pay')],
                }
    return None


def process_job_postings_data(job_postings_file, payroll_lookup_df):
    job_posting_cols = [
        "business_title",
        "salary_range_from",
        "salary_range_to",
        "posting_date",
        "post_until"
    ]
    processed_jobs_df = load_and_prepare_job_postings(job_postings_file, job_posting_cols)

    # Filter posting date for 2024 or 2025
    processed_jobs_df = processed_jobs_df.filter(
        (pl.col("posting_date").dt.year() == 2024) | (pl.col("posting_date").dt.year() == 2025)
    )
    
    candidate_strings = payroll_lookup_df["comparison_string"].unique().to_list()
    results = []
    title_match_count = 0

    for i, row in enumerate(processed_jobs_df.iter_rows(named=True), start=1):
        match = match_job_posting_to_payroll(row, candidate_strings, payroll_lookup_df)
        if match:
            results.append(match)
            title_match_count += 1  # Increment the counter for each match

        # Log every 1000 rows
        if i % 1000 == 0:
            logger.info(f"Processed {i} job postings... matches found so far: {len(results)}")

    # Log the total title matches after processing
    logger.info(f"Total title matches passing 85% threshold: {title_match_count}")

    return pl.DataFrame(results)


def write_csv_to_minio_stream(df, object_name="nyc_jobs_audited.csv"):
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


# ----------------------------
# Prefect Task
# ----------------------------
@task(name="fuzzy_match")
def fuzzy_match():
    tick = time.time()
    logger.info("Processing beginning on Fuzzy Matching NYC Jobs Postings & Payroll Data")

    logger.info("Processing payroll data to create comparison strings")
    processed_payroll_df = process_payroll_data("data/BRONZE/nyc_payroll_data_raw/")

    logger.info("Processing job postings data and applying fuzzy matching")
    job_postings_file = get_latest_file("data/BRONZE/nyc_job_postings_data_raw/")

    logger.info(f"Using job postings data file: {job_postings_file}")
    processed_jobs_df = process_job_postings_data(job_postings_file, processed_payroll_df)

    write_csv_to_minio_stream(processed_jobs_df)

    tock = time.time()
    logger.info(f"Fuzzy Matching completed in {tock - tick:.2f} seconds")

#  last run = 16.15 seconds
from minio import Minio
import os
import io
import polars as pl
from rapidfuzz import fuzz, process
from dotenv import load_dotenv
from logger import setup_logging
import time
from prefect import flow, task
from prefect.client.schemas.schedules import CronSchedule
logger = setup_logging()
load_dotenv()

def calculate_fuzzy_match(target_string, candidate_list_of_strings):
    best_match = process.extractOne(target_string, candidate_list_of_strings, scorer=fuzz.token_set_ratio)
    if best_match:
        return best_match[0], best_match[1]
    return None, 0

def process_payroll_data(payroll_file):
    payroll_cols = [
        "agency_name",
        "title_description",
        "base_salary",
        "pay_basis",
        "regular_gross_paid",
        "total_ot_paid",
        "total_other_pay"
    ]
    payroll_data_lazy = pl.scan_parquet(payroll_file).select(payroll_cols)
    payroll_data_lazy = payroll_data_lazy.with_columns([
        (pl.col("agency_name").cast(pl.Utf8) + pl.lit(" ") + pl.col("title_description").cast(pl.Utf8)).alias("comparison_string")
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
        .then(pl.col("posting_date") + pl.duration(days=30))
        .otherwise(pl.col("post_until"))
        .alias("post_until")
    ])
    jobs_lazy = jobs_lazy.with_columns([
        (pl.col("post_until") - pl.col("posting_date")).dt.total_days().alias("posting_duration")
    ])
    return jobs_lazy.collect()

def build_payroll_agency_lookup(payroll_lookup_df):
    logger.info("Pre-filtering payroll data by agency for efficiency.")
    lookup = {}
    for agency in payroll_lookup_df["agency_name"].unique():
        lookup[agency] = payroll_lookup_df.filter(
            pl.col("agency_name") == agency
        )["comparison_string"].to_list()
    return lookup

def match_job_posting_to_payroll(row, payroll_by_agency, payroll_lookup_df):
    agency = row['agency']
    target_string = row['business_title']
    candidate_strings = payroll_by_agency.get(agency, [])
    match_str, match_ratio = calculate_fuzzy_match(target_string, candidate_strings)
    logger.info(f"Fuzzy matching {target_string} against {len(candidate_strings)} payroll records.")
    if match_ratio >= 85:
        payroll_row = payroll_lookup_df.filter(
            (pl.col("agency_name") == agency) & (pl.col("comparison_string") == match_str)
        )
        if payroll_row.height > 0:
            pr = payroll_row.row(0)
            actual_regular_gross_paid = float(pr[payroll_lookup_df.columns.index('regular_gross_paid')])
            actual_total_ot_paid = float(pr[payroll_lookup_df.columns.index('total_ot_paid')])
            actual_total_other_pay = float(pr[payroll_lookup_df.columns.index('total_other_pay')])
            actual_total_income_paid = (
                actual_regular_gross_paid
                + actual_total_ot_paid
                + actual_total_other_pay
            )
            # Salary overlap check
            posting_salary_min = float(row['salary_range_from']) if row['salary_range_from'] is not None else 0
            posting_salary_max = float(row['salary_range_to']) if row['salary_range_to'] is not None else 0
            if posting_salary_min <= actual_regular_gross_paid <= posting_salary_max:
                return {
                    "agency": row['agency'],
                    "business_title": row['business_title'],
                    "posting_salary_range_from": posting_salary_min,
                    "posting_salary_range_to": posting_salary_max,
                    "posting_date": row['posting_date'],
                    "posting_until": row['post_until'],
                    "posting_duration": row['posting_duration'],
                    "actual_base_salary": pr[payroll_lookup_df.columns.index('base_salary')],
                    "actual_pay_basis": pr[payroll_lookup_df.columns.index('pay_basis')],
                    "actual_regular_gross_paid": actual_regular_gross_paid,
                    "actual_total_ot_paid": actual_total_ot_paid,
                    "actual_total_other_pay": actual_total_other_pay,
                    "actual_total_income_paid": actual_total_income_paid,
                    "match_ratio": match_ratio
                }
    return None

def process_job_postings_data(job_postings_file, payroll_lookup_df):
    job_posting_cols = [
        "agency",
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
    payroll_by_agency = build_payroll_agency_lookup(payroll_lookup_df)

    results = []
    for row in processed_jobs_df.iter_rows(named=True):
        match = match_job_posting_to_payroll(row, payroll_by_agency, payroll_lookup_df)
        if match:
            results.append(match)
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

# @flow(name="fuzzy_match")
@task(name="fuzzy_match")
def fuzzy_match():
    tick = time.time()
    logger.info("Processing beginning on Fuzzy Matching NYC Jobs Postings & Payroll Data")

    logger.info("Processing payroll data to create comparison strings")
    payroll_df = process_payroll_data("data/BRONZE/nyc_payroll_data_raw/ducklake-0198ae84-a79a-7628-bfa8-9481d369a09c.parquet")

    logger.info("Processing job postings data and applying fuzzy matching")
    processed_jobs_df = process_job_postings_data("data/BRONZE/nyc_job_postings_data_raw/ducklake-0198ae84-a5fd-7549-8a1d-287d56fbd9de.parquet", payroll_df)

    write_csv_to_minio_stream(processed_jobs_df)

    tock = time.time()
    logger.info(f"Fuzzy Matching completed in {tock - tick} seconds")

# if __name__ == "__main__":
#     fuzzy_match.serve(
#         name="Fuzzy_Matching",
#         schedule=CronSchedule(
#             cron="0 1 * * 0",
#             timezone="UTC"
#         ), # sundays at 1 am
#         tags=["data_ingestion", "weekly"]
#     )


# ratio: Compares the raw strings character by character. It’s strict and doesn’t handle word order or extra words well.
# token_sort_ratio: Sorts the words in both strings before comparing. Good for cases where word order differs but all words are present.
# token_set_ratio: Ignores word order and extra words, focusing on the intersection of words between the two strings. It’s best for matching job titles where titles may have extra descriptors or words in different orders (e.g., "Senior Data Analyst" vs "Data Analyst Senior").

# Last run time: ~8.7 minutes

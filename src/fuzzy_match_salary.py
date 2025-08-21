import os
import sys

current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)

from logger import setup_logging
from utils import (
    normalize_title,
    chunked,
    write_batch_to_parquet,
    merge_and_cleanup_batches,
    upload_parquet_and_remove_local,
    get_most_recent_file,
)
from db_sync import db_sync
from tqdm import tqdm
from rapidfuzz import process, fuzz
from datetime import datetime, timedelta
import polars as pl
import numpy as np

logger = setup_logging()

def posting_dates_handler(jobs, posting_key, until_key, date_fmt):
    null_value_fallback = 30

    for row_dict in jobs:
        if row_dict.get(until_key):
            continue

        posting_value = row_dict.get(posting_key)
        if not posting_value:
            continue

        try:
            posting_datetime = datetime.strptime(posting_value, "%Y-%m-%dT%H:%M:%S")
        except Exception:
            # minimal handling: skip non-matching strings
            continue

        row_dict[until_key] = (posting_datetime + timedelta(days=null_value_fallback)).strftime(date_fmt).upper()

    result = jobs
    return result


def apply_limit_to_matches(matches_by_job, jobs_data, payroll_data, limit, output_buffer):
    for job_index, match_list in matches_by_job.items():
        # lambda is an anonymous function that takes 1 tuple (x) and returns the second element (x[1])
        match_list = sorted(match_list, key=lambda x: x[1], reverse=True)[:limit]
        job_row = jobs_data[job_index]
        for payroll_index_global, match_score in match_list:
            payroll_row = payroll_data[payroll_index_global]
            payroll_salary = payroll_row["base_salary"]
            job_salary_min = job_row["salary_range_from"]
            job_salary_max = job_row["salary_range_to"]
            if (
                payroll_salary is not None
                and job_salary_min is not None
                and job_salary_max is not None
                and job_salary_min <= payroll_salary <= job_salary_max
            ):
                output_buffer.append({**job_row, **payroll_row, "score": match_score})


def fuzzy_match_payroll_to_jobs_vectorized(
    payroll_path,
    jobs_path,
    output_parquet,
    score_cutoff,
    token_set_threshold,
    limit,
    payroll_chunk_size,
    batch_size,
    year_start,
    year_end
):

    payroll_columns = [
        "title_description",
        "base_salary",
        "pay_basis",
        "regular_gross_paid",
        "total_ot_paid",
        "total_other_pay",
        "fiscal_year"
    ]
    jobs_columns = [
        "business_title",
        "salary_range_from",
        "salary_range_to",
        "posting_date",
        "post_until",
    ]

    payroll_file = get_most_recent_file(payroll_path)
    jobs_file = get_most_recent_file(jobs_path)

    payroll_df = pl.read_parquet(payroll_file, columns=payroll_columns)
    payroll_df = payroll_df.with_columns(
        pl.col("fiscal_year").cast(pl.Int32).alias("fiscal_year")
    )
    payroll_df = payroll_df.filter(pl.col("fiscal_year").is_between(int(year_start), int(year_end)))

    jobs_df = pl.read_parquet(jobs_file, columns=jobs_columns)

    # --- Thorough parsing/normalization for the remaining rows ---
    jobs_df = jobs_df.with_columns(pl.col("posting_date").cast(pl.Utf8).str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f", strict=False)).alias("posting_date_parsed")
    
    # After extracting years and collecting, require that parsing succeeded to ensure a canonical posting_date
    jobs_df = jobs_df.filter(pl.col("posting_date_parsed").is_not_null())

    jobs_df = jobs_df.with_columns(
        pl.col("posting_date_parsed").dt.strftime("%Y-%m-%dT%H:%M:%S").alias("posting_date")
    ).drop("posting_date_parsed")

    payroll_data = payroll_df.to_dicts()
    jobs_data = jobs_df.to_dicts()

    # Fill null post_until with posting_date + 30 days
    posting_dates_handler(jobs_data, "posting_date", "post_until", "%d-%b-%Y")

    payroll_titles_normalized = [normalize_title(row["title_description"]) for row in payroll_data]
    job_titles_normalized = [normalize_title(row["business_title"]) for row in jobs_data]

    # ---- Output schema ----
    output_schema = {
        "business_title": pl.Utf8,
        "salary_range_from": pl.Float64,
        "salary_range_to": pl.Float64,
        "posting_date": pl.Utf8,
        "post_until": pl.Utf8,
        "title_description": pl.Utf8,
        "base_salary": pl.Float64,
        "pay_basis": pl.Utf8,
        "regular_gross_paid": pl.Float64,
        "total_ot_paid": pl.Float64,
        "total_other_pay": pl.Float64,
        "score": pl.UInt8,
    }

    output_buffer = []
    batch_count = 0

    total_chunks = (len(payroll_titles_normalized) + payroll_chunk_size - 1) // payroll_chunk_size
    for start_index, end_index, payroll_titles_chunk in tqdm(
        chunked(payroll_titles_normalized, payroll_chunk_size),
        total=total_chunks,
        desc="Matching (vectorized, chunked)"
    ):
        # ---- Token set pre-filter ----
        similarity_matrix_token = process.cdist(
            job_titles_normalized,
            payroll_titles_chunk,
            scorer=fuzz.token_set_ratio,
            score_cutoff=token_set_threshold,
            workers=-1,
            dtype=np.uint8,
        )

        job_indices, chunk_payroll_indices = np.nonzero(similarity_matrix_token)
        if job_indices.size == 0:
            continue

        matches_by_job = {}
        for job_index, payroll_index_local in zip(job_indices, chunk_payroll_indices):
            payroll_index_global = start_index + int(payroll_index_local)
            # ---- Full WRatio on filtered candidates ----
            wscore = fuzz.WRatio(
                job_titles_normalized[job_index],
                payroll_titles_normalized[payroll_index_global]
            )
            if wscore >= score_cutoff:
                job_row = jobs_data[job_index]
                payroll_row = payroll_data[payroll_index_global]

                # ---- Salary filter ----
                payroll_salary = payroll_row["base_salary"]
                job_salary_min = job_row["salary_range_from"]
                job_salary_max = job_row["salary_range_to"]

                if (
                    payroll_salary is not None
                    and job_salary_min is not None
                    and job_salary_max is not None
                    and job_salary_min <= payroll_salary <= job_salary_max
                ):
                    if limit is None:
                        output_buffer.append({**job_row, **payroll_row, "score": wscore})
                    else:
                        matches_by_job.setdefault(job_index, []).append((payroll_index_global, wscore))

        # ---- Apply limit if specified ----
        if limit is not None:
            apply_limit_to_matches(matches_by_job, jobs_data, payroll_data, limit, output_buffer)

        # ---- Batch write to separate Parquet files ----
        if len(output_buffer) >= batch_size:
            batch_count = write_batch_to_parquet(output_buffer, output_schema, output_parquet, batch_count)

    # Flush last batch
    if output_buffer:
        batch_count = write_batch_to_parquet(output_buffer, output_schema, output_parquet, batch_count)

    logger.info(f"Fuzzy matching complete. {batch_count} batch files written.")

    # ---- Merge all batch files into single Parquet ----
    merge_and_cleanup_batches(output_parquet, logger)
    # upload final parquet to MinIO and delete local copy
    upload_parquet_and_remove_local(output_parquet, logger)
    logger.info(
        "Notes:\n"
        f" - Compared {len(job_titles_normalized):,} job titles against {len(payroll_titles_normalized):,} payroll titles.\n"
        f" - Score cutoff (WRatio): {score_cutoff}\n"
        f" - Token set threshold: {token_set_threshold}\n"
        f" - Salary filter applied: only keep payroll salaries within job range\n"
        f" - Limit per job: {limit}\n"
        f" - Payroll chunk size: {payroll_chunk_size}\n"
        f" - Written in batches of {batch_size} rows.\n"
        " - Non-matches or salary mismatches are skipped.\n"
        " - Normalization applied: lowercase, no punctuation, single spaces."
    )

if __name__ == "__main__":
    fuzzy_match_payroll_to_jobs_vectorized(
        payroll_path="data/BRONZE/nyc_payroll_data/",
        jobs_path="data/BRONZE/nyc_job_postings_data/",
        output_parquet="data/BRONZE/payroll_to_jobs_title_fuzzy_matches.parquet",
        score_cutoff=85,
        token_set_threshold=85,
        limit=None,
        payroll_chunk_size=100_000,
        batch_size=100_000,
        year_start=2024,
        year_end=2025
    )


# Token set pre-filter (threshold 85)
# WRatio matching
# Salary filtering (only keep payroll salaries within job ranges)
# Null values in date handling
# Batch Parquet writes
# Multi-core processing
# Normalization
# Optional per-job limit
# Merging all batch files into a single final Parquet
# Automatic deletion of temporary batch files

# Run time Total for No Limit 2.0 : 2:23:19 | Total Returned Results: 8,737,221
# Run time Total for No Limit 2.1 : 12:47 | Total Returned Results: 562,898
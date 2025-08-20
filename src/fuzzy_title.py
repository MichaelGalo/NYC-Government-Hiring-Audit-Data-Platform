import os
import sys
import re
import string
from typing import List, Dict, Iterable
import glob

current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)

from logger import setup_logging
from tqdm import tqdm
from rapidfuzz import process, fuzz
import polars as pl
import numpy as np

logger = setup_logging()

# ----------------------------
# Normalization helper
# ----------------------------
punctuation_table = str.maketrans("", "", string.punctuation)

def normalize_title(title: str) -> str:
    """Normalize job/payroll titles for fuzzy matching only."""
    if not isinstance(title, str):
        return ""
    title = title.lower()
    title = title.translate(punctuation_table)
    title = re.sub(r"\s+", " ", title)
    return title.strip()


# ----------------------------
# Chunk utility
# ----------------------------
def chunked(iterable: List, size: int) -> Iterable[tuple[int, int, List]]:
    total_length = len(iterable)
    for start_index in range(0, total_length, size):
        end_index = min(start_index + size, total_length)
        yield start_index, end_index, iterable[start_index:end_index]


# ----------------------------
# Main function
# ----------------------------
def fuzzy_match_payroll_to_jobs_vectorized(
    payroll_path: str = "alt_data/nyc_payroll_data.parquet",
    jobs_path: str = "alt_data/nyc_job_postings_data.parquet",
    output_parquet: str = "alt_data/payroll_to_jobs_title_fuzzy_matches.parquet",
    score_cutoff: int = 85,
    token_set_threshold: int = 85,
    limit: int | None = None,
    payroll_chunk_size: int = 100_000,
    batch_size: int = 100_000,
):
    if not os.path.exists(payroll_path):
        raise FileNotFoundError(f"File not found: {payroll_path}")
    if not os.path.exists(jobs_path):
        raise FileNotFoundError(f"File not found: {jobs_path}")

    payroll_columns = [
        "title_description",
        "base_salary",
        "pay_basis",
        "regular_gross_paid",
        "total_ot_paid",
        "total_other_pay",
    ]
    jobs_columns = [
        "business_title",
        "salary_range_from",
        "salary_range_to",
        "posting_date",
        "post_until",
    ]

    payroll_df = pl.read_parquet(payroll_path, columns=payroll_columns)
    jobs_df = pl.read_parquet(jobs_path, columns=jobs_columns)

    payroll_data: List[Dict] = payroll_df.to_dicts()
    jobs_data: List[Dict] = jobs_df.to_dicts()

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

    output_buffer: List[Dict] = []
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

        matches_by_job: Dict[int, List[tuple[int, int]]] = {}
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
            for job_index, match_list in matches_by_job.items():
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

        # ---- Batch write to separate Parquet files ----
        if len(output_buffer) >= batch_size:
            for row in output_buffer:
                for date_col in ["posting_date", "post_until"]:
                    if row[date_col] is not None:
                        row[date_col] = str(row[date_col])
            batch_filename = output_parquet.replace(".parquet", f"_batch_{batch_count:03}.parquet")
            pl.DataFrame(output_buffer, schema=output_schema).write_parquet(batch_filename)
            output_buffer.clear()
            batch_count += 1

    # Flush last batch
    if output_buffer:
        for row in output_buffer:
            for date_col in ["posting_date", "post_until"]:
                if row[date_col] is not None:
                    row[date_col] = str(row[date_col])
        batch_filename = output_parquet.replace(".parquet", f"_batch_{batch_count:03}.parquet")
        pl.DataFrame(output_buffer, schema=output_schema).write_parquet(batch_filename)
        output_buffer.clear()
        batch_count += 1

    logger.info(f"Fuzzy matching complete. {batch_count} batch files written.")

    # ---- Merge all batch files into single Parquet ----
    batch_files_pattern = output_parquet.replace(".parquet", "_batch_*.parquet")
    batch_files = sorted(glob.glob(batch_files_pattern))
    if batch_files:
        logger.info(f"Merging {len(batch_files)} batch files into final Parquet...")
        merged_df = pl.concat([pl.read_parquet(f) for f in batch_files])
        merged_df.write_parquet(output_parquet)
        logger.info(f"Final Parquet written to {output_parquet}")

        # Delete temporary batch files
        for f in batch_files:
            os.remove(f)
        logger.info("Temporary batch files deleted.")
    else:
        logger.warning("No batch files found to merge.")

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
        score_cutoff=85,
        token_set_threshold=85,
        limit=None,
        payroll_chunk_size=100_000,
        batch_size=100_000,
    )


# Token set pre-filter (threshold 85)
# WRatio matching
# Salary filtering (only keep payroll salaries within job ranges)
# Batch Parquet writes
# Multi-core processing
# Normalization
# Optional per-job limit
# Merging all batch files into a single final Parquet
# Automatic deletion of temporary batch files

# Run time Total for No Limit: 2:23:19
# Total Returned Results for No Limit | No Distinct: 8,737,221
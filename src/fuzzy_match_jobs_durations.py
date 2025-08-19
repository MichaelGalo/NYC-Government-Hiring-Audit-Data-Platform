import polars as pl
from rapidfuzz import fuzz, process
from prefect import task
from dotenv import load_dotenv
from logger import setup_logging
import time
from utils import normalize_string, get_latest_file, write_csv_to_minio_stream, write_dataframe_to_bronze_table

logger = setup_logging()
load_dotenv()


def calculate_fuzzy_match(target_string, candidate_lookup):
    target_norm = normalize_string(target_string)
    candidates = list(candidate_lookup.keys())

    best_match = process.extractOne(
        target_norm,
        candidates,
        scorer=fuzz.token_set_ratio
    )

    if best_match:
        norm_match, score = best_match[0], best_match[1]
        original_match = candidate_lookup[norm_match]
        return original_match, score
    return None, 0


def process_audited_jobs(directory):
    audited_jobs_file = get_latest_file(directory)
    logger.info(f"Using audited jobs data file: {audited_jobs_file}")

    audited_jobs_data_lazy = pl.scan_parquet(audited_jobs_file).select("job_title")
    audited_jobs_data_lazy = audited_jobs_data_lazy.with_columns(
        pl.col("job_title").cast(pl.Utf8).alias("comparison_string")
    )
    result = audited_jobs_data_lazy.collect()
    return result


def process_lightcast_data(directory):
    lightcast_file = get_latest_file(directory)
    logger.info(f"Using lightcast data file: {lightcast_file}")

    lightcast_cols = [
        "Occupation (SOC)",
        "Total Postings (Jan 2024 - Jun 2025)",
        "Median Posting Duration"
    ]
    lightcast_data_lazy = pl.scan_parquet(lightcast_file).select(lightcast_cols)
    lightcast_data_lazy = lightcast_data_lazy.with_columns(
        pl.col("Occupation (SOC)").cast(pl.Utf8).alias("comparison_string")
    )
    result = lightcast_data_lazy.collect()
    return result


def match_job_titles_to_occupations(audited_jobs_df, lightcast_df):
    # Creates a lookup dictionary for normalized strings to original values to return original matches
    candidate_lookup = {}

    unique_candidates = lightcast_df["comparison_string"].unique().to_list()
    for candidate_string in unique_candidates:
        normalized_candidate = normalize_string(candidate_string)
        candidate_lookup[normalized_candidate] = candidate_string


    results = []

    for row in audited_jobs_df.iter_rows(named=True):
        target_string = row["comparison_string"]
        match_str, match_ratio = calculate_fuzzy_match(target_string, candidate_lookup)

        if match_ratio >= 75:
            lightcast_row = lightcast_df.filter(pl.col("comparison_string") == match_str)
            if lightcast_row.height > 0:
                lc = lightcast_row.row(0)
                results.append({
                    "job_title": row["job_title"],
                    "lightcast_matched_occupation": match_str, 
                    "total_postings": lc[lightcast_df.columns.index("Total Postings (Jan 2024 - Jun 2025)")],
                    "median_posting_duration": lc[lightcast_df.columns.index("Median Posting Duration")]
                })

    logger.info(f"Total matches found: {len(results)}")
    result = pl.DataFrame(results)
    return result


@task(name="fuzzy_match_job_durations")
def fuzzy_match_jobs_duration():
    tick = time.time()
    logger.info("Starting fuzzy matching for job durations")

    logger.info("Processing audited jobs data")
    audited_jobs_df = process_audited_jobs("data/BRONZE/nyc_jobs_audited_raw/")

    logger.info("Processing Lightcast data")
    lightcast_df = process_lightcast_data("data/BRONZE/lightcast_top_posted_occupations_SOC_raw/")

    logger.info("Matching job titles to occupations")
    results_df = match_job_titles_to_occupations(audited_jobs_df, lightcast_df)

    logger.info("Writing matched results to BRONZE table")
    write_dataframe_to_bronze_table(results_df, "job_durations_fuzzy")

    logger.info("Streaming matched results to MinIO as CSV")
    write_csv_to_minio_stream(results_df, object_name="job_durations_fuzzy.csv")

    tock = time.time()
    logger.info(f"Fuzzy matching completed in {tock - tick:.2f} seconds")

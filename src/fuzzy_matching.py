import polars as pl
from rapidfuzz import fuzz, process
from logger import setup_logging

logger = setup_logging()

def match_job_titles(payroll_df, job_postings_df):
    # Extract relevant columns (2 listed)
    payroll_titles = payroll_df['title_description'].tolist()
    job_posting_titles = job_postings_df['business_title'].tolist()

    # Create a mapping of job titles to their best matches
    title_mapping = {}
    for title in payroll_titles:
        best_match = process.extractOne(title, job_posting_titles, scorer=fuzz.token_sort_ratio)
        title_mapping[title] = best_match

    return title_mapping

# dataset 1: payroll
payroll_df = pl.read_parquet("data/BRONZE/nyc_payroll_data_raw/ducklake-0198ae84-a79a-7628-bfa8-9481d369a09c.parquet")

# dataset 2: job postings
job_postings_df = pl.read_parquet("data/BRONZE/nyc_job_postings_data_raw/ducklake-0198ae84-a5fd-7549-8a1d-287d56fbd9de.parquet")




if __name__ == "__main__":
    print(f"Here are the first few results from payroll: {payroll_df.head()}")
    print(f"Here are the first few results from job postings: {job_postings_df.head()}")
-- Here will be the SQL code to create the cleaned tables that will be looped over in main.py
CREATE TABLE IF NOT EXISTS GOLD.nyc_salary_matches AS
SELECT
    business_title AS posted_job_title,
    title_description AS matched_actual_payroll_title,
    score AS match_score,
    salary_range_from AS posting_min_salary,
    salary_range_to AS posting_max_salary,
    base_salary AS actual_base_salary,
    CAST(CAST(strptime(post_until, '%d-%b-%Y') AS DATE) - CAST(posting_date AS DATE) AS INTEGER) AS posting_duration_days,
    regular_gross_paid AS actual_gross_paid,
    total_ot_paid AS actual_ot_paid,
    total_other_pay AS actual_other_pay
FROM BRONZE.jobs_to_lightcast_title_fuzzy_matches
ORDER BY match_score DESC;

CREATE TABLE IF NOT EXISTS GOLD.nyc_matched_job_posting_duration_SOC AS
SELECT
    business_title AS title,
    lightcast_matched_occupation,
    "Total Postings (Jan 2024 - Jun 2025)" AS total_postings,
    "Median Posting Duration" AS median_posting_duration
FROM BRONZE.jobs_to_lightcast_title_fuzzy_matches
ORDER BY median_posting_duration DESC;
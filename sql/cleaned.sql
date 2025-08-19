-- Here will be the SQL code to create the cleaned tables that will be looped over in main.py
CREATE TABLE IF NOT EXISTS GOLD.nyc_salary_matches AS
SELECT
    job_title AS posted_job_title,
    matched_payroll_title AS matched_actual_payroll_title,
    match_ratio,
    posting_salary_range_from AS posting_min_salary,
    posting_salary_range_to AS posting_max_salary,
    actual_base_salary,
    posting_duration AS posting_duration_days,
    actual_regular_gross_paid AS actual_gross_paid,
    actual_total_ot_paid AS actual_ot_paid,
    actual_total_other_pay AS actual_other_pay
FROM
    BRONZE.nyc_jobs_audited_fuzzy_raw;

CREATE TABLE IF NOT EXISTS GOLD.nyc_matched_job_posting_duration_SOC AS
SELECT 
    job_title AS actual_payroll_job_title,
    lightcast_matched_occupation,
    total_postings,
    median_posting_duration
FROM BRONZE.job_durations_fuzzy_raw;
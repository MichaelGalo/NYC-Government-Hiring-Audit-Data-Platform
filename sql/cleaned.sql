-- Here will be the SQL code to create the cleaned tables that will be looped over in main.py
CREATE TABLE IF NOT EXISTS GOLD.nyc_salary_match_ratio AS
SELECT
    _record_id AS id,
    business_title,
    match_ratio,
    posting_salary_range_from AS posting_min_salary,
    posting_salary_range_to AS posting_max_salary,
    actual_base_salary,
    actual_regular_gross_paid AS actual_gross_paid,
    actual_total_ot_paid AS actual_ot_paid,
    actual_total_other_pay AS actual_other_pay
FROM
    BRONZE.nyc_jobs_audited_raw

CREATE TABLE IF NOT EXISTS GOLD.nyc_job_posting_duration_SOC AS
SELECT 
    m.job_title,
    l."Occupation (SOC)" AS occupation_title,
    l."Total Postings (Jan 2024 - Jun 2025)" AS total_postings,
    l."Median Posting Duration" AS median_posting_duration
FROM GOLD.nyc_salary_match_ratio AS m
LEFT JOIN BRONZE.lightcast_top_posted_occuipations_SOC_raw AS l ON m.job_title = l."Occupation (SOC)"
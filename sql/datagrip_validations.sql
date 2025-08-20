-- Connect to the db, not the catalog

LOAD ducklake;
ATTACH 'ducklake:/Users/michaelgalo/Workspace/data-engineering/projects/Core_Group_Projects/project-4-nyc-hiring-audit/catalog.ducklake'
AS my_ducklake (DATA_PATH '/Users/michaelgalo/Workspace/data-engineering/projects/Core_Group_Projects/project-4-nyc-hiring-audit/data');
USE my_ducklake;

-- Validation of Bronze Table Creation
SELECT * FROM BRONZE.lightcast_top_posted_occupations_soc_raw
SELECT * FROM BRONZE.nyc_job_postings_data_raw
SELECT * FROM BRONZE.nyc_payroll_data_raw

-- Validation of Fuzzy Matching
SELECT * FROM BRONZE.payroll_to_jobs_title_fuzzy_matches
SELECT * FROM BRONZE.jobs_to_lightcast_title_fuzzy_matches

-- Validation of Gold Table Creation
SELECT * FROM GOLD.nyc_salary_matches
SELECT * FROM GOLD.nyc_matched_job_posting_duration_SOC
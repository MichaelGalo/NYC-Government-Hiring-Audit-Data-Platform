-- Connect to the db, not the catalog

LOAD ducklake;
ATTACH 'ducklake:/Users/michaelgalo/Workspace/data-engineering/projects/Core_Group_Projects/project-4-nyc-hiring-audit/catalog.ducklake'
AS my_ducklake (DATA_PATH '/Users/michaelgalo/Workspace/data-engineering/projects/Core_Group_Projects/project-4-nyc-hiring-audit/data');
USE my_ducklake;

-- Validation of Bronze Table Creation
SELECT * FROM BRONZE.lightcast_top_posted_job_titles_raw
SELECT * FROM BRONZE.lightcast_top_posted_occupations_raw
SELECT * FROM BRONZE.lightcast_top_posted_occupations_onet_raw
SELECT * FROM BRONZE.lightcast_top_posted_occupations_soc_raw

-- api raw sources
SELECT * FROM BRONZE.nyc_job_postings_data_raw
SELECT * FROM BRONZE.nyc_payroll_data_raw

-- Validation of Fuzzy Matching
SELECT * FROM BRONZE.nyc_jobs_audited_fuzzy_raw
SELECT * FROM BRONZE.job_durations_fuzzy_raw

-- Alt Validation of Fuzzy Matching 2.0
SELECT *
FROM read_parquet('/Users/michaelgalo/Workspace/data-engineering/projects/Core_Group_Projects/project-4-nyc-hiring-audit/alt_data/payroll_to_jobs_title_fuzzy_matches.parquet')

SELECT *
FROM read_parquet('/Users/michaelgalo/Workspace/data-engineering/projects/Core_Group_Projects/project-4-nyc-hiring-audit/alt_data/jobs_to_lightcast_title_fuzzy_matches.parquet')

-- Validation of Gold Table Creation
SELECT * FROM GOLD.nyc_salary_matches
SELECT * FROM GOLD.nyc_matched_job_posting_duration_SOC
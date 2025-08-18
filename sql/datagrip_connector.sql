-- Connect to the db, not the catalog

LOAD ducklake;
ATTACH 'ducklake:/Users/michaelgalo/Workspace/data-engineering/projects/Core_Group_Projects/project-4-nyc-hiring-audit/catalog.ducklake'
AS my_ducklake (DATA_PATH '/Users/michaelgalo/Workspace/data-engineering/projects/Core_Group_Projects/project-4-nyc-hiring-audit/data');
USE my_ducklake;

-- Validation of Table Creation
SELECT * FROM BRONZE.nyc_job_postings_data_raw
SELECT * FROM BRONZE.nyc_payroll_data_raw
SELECT * FROM BRONZE.lightcast_executive_summary_raw
SELECT * FROM BRONZE.nyc_jobs_audited_raw
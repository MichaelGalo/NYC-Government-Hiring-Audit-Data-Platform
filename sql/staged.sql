-- Here will be the SQL code to create the staged tables that will be looped over in main.py
CREATE TABLE IF NOT EXISTS SILVER.nyc_job_postings_data_staged AS
SELECT 
    *
FROM BRONZE.nyc_job_postings_data_raw;

CREATE TABLE IF NOT EXISTS SILVER.nyc_payroll_data_staged AS
SELECT 
    *
FROM BRONZE.nyc_payroll_data_raw;

CREATE TABLE IF NOT EXISTS SILVER.lightcast_executive_summary AS
SELECT
    *
FROM BRONZE.lightcast_executive_summary_raw;


-- TODO: select only the relevant columns do any cleaning
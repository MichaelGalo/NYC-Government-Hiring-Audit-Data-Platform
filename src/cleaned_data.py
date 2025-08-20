from prefect import flow, task
from dotenv import load_dotenv
from logger import setup_logging
from prefect.client.schemas.schedules import CronSchedule
import time
import os
import sys
import duckdb
current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)
load_dotenv()
logger = setup_logging()

@flow(name="business_logic_aggregation")
def run_gold_layer():
    gold_start_time = time.time()

    duckdb.install_extension("ducklake")
    db_path = os.path.join(parent_path, "nyc_jobs_audit.db")

    con = duckdb.connect(db_path)
    logger.info(f"Connected to persistent DuckDB database: {db_path}")

    data_path = os.path.join(parent_path, "data")
    catalog_path = os.path.join(parent_path, "catalog.ducklake")

    logger.info(f"Attaching DuckLake with data path: {data_path}")
    con.execute(f"ATTACH 'ducklake:{catalog_path}' AS my_ducklake (DATA_PATH '{data_path}')")
    con.execute("USE my_ducklake")
    logger.info("DuckLake attached and activated successfully")

    logger.info("Running cleaned SQL script(s)")

    with open("sql/cleaned.sql", "r") as f:
        query = f.read()
    con.execute(query)

    logger.info("Gold layer processing completed successfully")

    con.close()
    gold_end_time = time.time()
    logger.info(f"Gold layer processing completed in {gold_end_time - gold_start_time:.2f} seconds")

if __name__ == "__main__":
    run_gold_layer.serve(
        name="Business_Logic_Aggregation",
        schedule=CronSchedule(
            cron="0 4 * * 0",
            timezone="UTC"
        ), # sundays at 4 am
        tags=["business_logic", "weekly"]
    )
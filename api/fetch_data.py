import os
import sys
current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)
from fastapi import HTTPException
import duckdb
from src.logger import setup_logging
import duckdb

logger = setup_logging()

DATASET_CONFIG = {
    0: {
        "table_name": "GOLD.nyc_salary_matches"
    },
    1: {
        "table_name": "GOLD.nyc_matched_job_posting_duration_SOC"
    },
    2: {
        "table_name": "GOLD.nyc_salary_matches_unique_job_posting_title"
    },
    3: {
        "table_name": "GOLD.nyc_matched_job_posting_duration_SOC_unique_title"
    }
}

def fetch_single_dataset(dataset_id, offset, limit):
    try:
        dataset_id = int(dataset_id)
        offset = int(offset)
        limit = int(limit)
        logger.info(f"Fetching dataset {dataset_id} with offset={offset}, limit={limit}")
        
        # Validate dataset_id
        if dataset_id not in DATASET_CONFIG:
            raise ValueError(f"Invalid dataset_id: {dataset_id}")
        
        dataset = DATASET_CONFIG[dataset_id]
        logger.info(f"Using dataset: {dataset['table_name']}")
        
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

        # Use a fully parameterized query
        query = f"""
            SELECT * FROM {dataset['table_name']}
            OFFSET ?
            LIMIT ?
        """
        logger.info(f"Executing parameterized query on table: {dataset['table_name']}")
        result = con.execute(query, [offset, limit]).fetchall()
        columns = [desc[0] for desc in con.description]

        data = [dict(zip(columns, row)) for row in result]

        logger.info(f"Retrieved {len(data)} records")
        return data
        
    except ValueError as ve:
        logger.error(f"ValueError: {ve}")
        raise HTTPException(status_code=400, detail=str(ve))
    except KeyError as ke:
        logger.error(f"KeyError: {ke}")
        raise HTTPException(status_code=404, detail="Dataset not found")
    except Exception as e:
        logger.error(f"Error fetching dataset {dataset_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        con.close()


def get_reports_list():
    result = []
    for dataset_id, config in DATASET_CONFIG.items():
        # Extract the table name and strip the 'GOLD.' prefix
        stripped_table_name = config["table_name"].split("GOLD.")[-1]
        
        result.append({
            "id": dataset_id,
            "report": stripped_table_name
        })
    return result
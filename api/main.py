import os
import sys
current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)
from fastapi import FastAPI, HTTPException
from src.logger import setup_logging
from fetch_data import get_reports_list, fetch_single_dataset
import datetime

app = FastAPI()
logger = setup_logging()

@app.get("/", tags=["Root"])
def read_root():
    try:
        logger.info("Root endpoint accessed")
        return {"message": "Welcome to the NYC Jobs Audit API. Please visit '/docs' for documentation on how to use this API."}
    except Exception as e:
        logger.error(f"Error accessing root endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/health", tags=["Health"])
def read_health():
    try: 
        logger.info("Health endpoint accessed")
        return {"status": "healthy",
                "timestamp": datetime.datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"Error fetching health status: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/reports", tags=["Reports"])
def read_reports_list():
    try:
        reports = get_reports_list()
        return reports
    except Exception as e:
        logger.error(f"Error fetching reports list: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/reports/{report_id}", tags=["Reports"])
def read_report(report_id):
    try:
        report = fetch_single_dataset(report_id, 0, 750000)
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        return report
    except Exception as e:
        logger.error(f"Error fetching report {report_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

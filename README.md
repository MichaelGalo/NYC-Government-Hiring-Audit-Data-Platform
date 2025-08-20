# NYC Government Hiring Audit Data Platform

## Project Overview
This project builds a comprehensive data pipeline to audit NYC government hiring practices. The pipeline integrates data from multiple sources, including NYC Open Data (Payroll, Job Postings) and Lighthouse Data Analytics, to reconcile job titles using fuzzy string matching and calculate key audit metrics. The final datasets are delivered to DuckDB and an API for analysis, ensuring a 5-day turnaround for actionable insights.

## Deliverables
- **Job Salary Match Ratios**: Dataset comparing job postings and payroll data.
- **Job Posting Duration Dataset**: Dataset analyzing job posting durations using Lightcast data.
- **Additional Reports**: Potential additional datasets based on Lightcast data.

## Learning Objectives
- Design and implement a concise data pipeline for multi-source audit data.
- Acquire data from CSVs (public APIs) and XLSX files.
- Implement fuzzy string matching to reconcile textual data.
- Calculate derived metrics for the gold layer: job posting duration and salary match ratios.
- Orchestrate the pipeline using Prefect.
- Utilize logging and error handling for critical pipeline stages.
- Document pipeline methodology and key findings.
- Expose and Present Data

## Data Architecture
Utilizing a modern ducklake approach, this pipeline is organized into two main tiers:

### Bronze Layer
- **Purpose**: Raw data ingestion and storage.
- **Data Sources**:
  - NYC Payroll Data
  - NYC Job Postings Data
  - Lighthouse Analytics Data
- **Storage**: Data is loaded into DuckDB for fast local processing.

### Gold Layer
- **Purpose**: Data cleaning, transformation, and metric calculation.
- **Key Processes**:
  - Filtering for relevant posting dates (2024/2025).
  - Normalization and basic transformations.
  - Fuzzy string matching to reconcile job titles.
  - Calculation of match ratios, posting durations, and salary metrics.
- **Output**: Final audit datasets for analysis.

## Technologies Used
- **Programming Language**: Python
- **Database**: DuckDB & Ducklake
- **Orchestration**: Prefect
- **Fuzzy Matching**: rapidfuzz
- **API**: FastAPI (for exposing data)
- **Dashboard**: Streamlit (in development))

## Terminal Commands

```bash
# API Development Server
fastapi dev api/main.py
```

```bash
# Streamlit Data Vis
streamlit run streamlit/app.py
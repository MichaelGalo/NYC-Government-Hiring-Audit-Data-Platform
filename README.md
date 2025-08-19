# NYC Government Hiring Audit Data Platform

## Overview
This project builds a core data pipeline to support an audit of NYC government hiring practices. The pipeline ingests, cleans, and transforms data from NYC Open Data (Payroll, Job Postings) and a Lighthouse Data Analytics Excel file. The main challenge is integrating these sources, reconciling job titles using fuzzy string matching, and calculating key audit metrics. The final datasets are delivered to PostgreSQL (or DuckDB) for analysis, with a 5-day turnaround.

## Learning Objectives
- Design and implement a concise data pipeline for multi-source audit data.
- Acquire data from CSVs (public APIs) and XLSX files.
- Implement fuzzy string matching to reconcile textual data.
- Calculate derived metrics for the gold layer: job posting duration and salary match ratios.
- Orchestrate the pipeline using Prefect.
- Utilize logging and error handling for critical pipeline stages.
- Document pipeline methodology and key findings.
- Expose and Present Data 

## Pipeline Architecture
The pipeline consists of 2 main tiers:

### Bronze Layer
- Raw ingestion of NYC Payroll, Job Postings, and Lighthouse Analytics Data.
- Data is loaded into DuckDB for fast local processing.

### Gold Layer 
- Filtering for relevant posting dates (2024/2025).
- Data cleaning, normalization, and basic transformations.
- Fuzzy string matching to reconcile job titles.
- Calculation of match ratios, posting durations, and salary metrics.
- Final audit datasets for analysis.

## Orchestration
The entire workflow will be orchestrated using Prefect.

## Future Extensions
- **FastAPI**: Expose cleaned and gold-tier data via a REST API for downstream analysis and reporting.
- **Streamlit Dashboard**: If time permits, build an interactive dashboard for data exploration and visualization.

## Key Technologies
- Python 
- DuckDB & Ducklake
- Prefect (orchestration)
- rapidfuzz
- FastAPI (in development)
- Streamlit (in development)
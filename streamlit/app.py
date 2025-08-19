import os
import sys
current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)
from api.fetch_data import fetch_single_dataset
import streamlit as st

st.title("NYC Hiring Audit Data Visualization")

nyc_salary_matches = fetch_single_dataset(0, 0, 5000)
st.subheader("Job Posting & Payroll: Title & Salary Matches")
st.dataframe(nyc_salary_matches)

nyc_matched_job_posting_duration_SOC = fetch_single_dataset(1, 0, 5000)
st.subheader("Matched Job/Posting Posting Duration")
st.dataframe(nyc_matched_job_posting_duration_SOC)

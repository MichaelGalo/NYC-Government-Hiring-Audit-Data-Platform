import os
import sys
current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)
from api.fetch_data import fetch_single_dataset
import pandas as pd
import polars as pl
import streamlit as st


@st.cache_data(ttl=300)
def load_dataset(dataset_id, offset, limit):
    try:
        data = fetch_single_dataset(dataset_id, offset, limit)
        return data or []
    except Exception as exc:  # keep simple for the UI
        st.error(f"Error loading dataset {dataset_id}: {exc}")
        return []


def to_polars(rows):
    try:
        return pl.DataFrame(rows)
    except Exception:
        return pl.from_pandas(pd.DataFrame(rows))


def main():
    st.set_page_config(page_title="NYC Hiring Audit — Viewer", layout="wide")
    st.title("NYC Hiring Audit")

    with st.spinner("Loading datasets..."):        
        ds2 = load_dataset(2, 0, 750000)
        df2 = to_polars(ds2)

        ds3 = load_dataset(3, 0, 750000)
        df3 = to_polars(ds3)

    st.header("Job Posting & Payroll: Unique Title & Salary Matches")
    if df2.is_empty():
        st.info("No rows returned for dataset 2")
    else:
        if "match_score" in df2.columns:
            try:
                df2 = df2.with_columns(pl.col("match_score").cast(pl.Float64))
            except Exception:
                pass

            try:
                df2_sorted = df2.sort("match_score", reverse=True)
            except Exception:
                df2_sorted = df2

            try:
                min_score = float(df2_sorted["match_score"].min())
                max_score = float(df2_sorted["match_score"].max())
            except Exception:
                min_score, max_score = 0.0, 100.0

            if min_score >= max_score:
                st.info(f"All rows have match_score = {min_score:.2f}")
                score_range = (min_score, max_score)
            else:
                score_range = st.slider(
                    "match_score range (unique titles)",
                    min_value=min_score,
                    max_value=max_score,
                    value=(min_score, max_score),
                    format="%.2f"
                )

            filtered = df2_sorted.filter((pl.col("match_score") >= score_range[0]) & (pl.col("match_score") <= score_range[1]))
            try:
                filtered = filtered.sort("match_score", reverse=True)
            except Exception:
                pass
        else:
            df2_sorted = df2
            filtered = df2

        st.markdown(f"**Showing {filtered.height} rows** (filtered from {df2_sorted.height})")
        try:
            st.dataframe(filtered.to_pandas().reset_index(drop=True))
        except Exception:
            st.dataframe(filtered)

        if "match_score" in df2.columns and filtered.height > 0:
            try:
                avg_score = float(filtered["match_score"].mean())
                st.metric("Average match_score (filtered, unique titles)", f"{avg_score:.1f}")
            except Exception:
                pass

    st.header("Unique Matched Job Posting Duration (SOC)")
    if df3.is_empty():
        st.info("No rows returned for dataset 3")
    else:
        if "median_posting_duration" in df3.columns:
            try:
                df3_sorted = df3.sort("median_posting_duration", reverse=True)
            except Exception:
                df3_sorted = df3
        else:
            df3_sorted = df3

        st.markdown(f"**Rows: {df3_sorted.height}**")

        try:
            st.dataframe(df3_sorted.to_pandas())
        except Exception:
            st.dataframe(df3_sorted)

if __name__ == "__main__":
    main()


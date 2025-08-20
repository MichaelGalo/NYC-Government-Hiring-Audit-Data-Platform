import os
import sys
import polars as pl
import streamlit as st

current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)
from api.fetch_data import fetch_single_dataset


@st.cache_data(ttl=300)
def load_dataset(dataset_id, offset, limit):
    try:
        data = fetch_single_dataset(dataset_id, offset, limit)
        return data or []
    except Exception as exc:  # keep simple for the UI
        st.error(f"Error loading dataset {dataset_id}: {exc}")
        return []


def to_polars(rows):
    if not rows:
        return pl.DataFrame()
    try:
        return pl.DataFrame(rows)
    except Exception:
        import pandas as pd
        return pl.from_pandas(pd.DataFrame(rows))


def main():
    st.set_page_config(page_title="NYC Hiring Audit — Viewer", layout="wide")
    st.title("NYC Hiring Audit")

    with st.spinner("Loading datasets..."):
        ds0 = load_dataset(0, 0, 750000)
        df0 = to_polars(ds0)

        ds1 = load_dataset(1, 0, 5000)
        df1 = to_polars(ds1)

    # --- Dataset 0: salary matches (filter on match_score) ---
    st.header("Job Posting & Payroll: Title & Salary Matches")
    if df0.is_empty():
        st.info("No rows returned for dataset 0")
    else:
        if "match_score" in df0.columns:
            try:
                df0 = df0.with_columns(pl.col("match_score").cast(pl.Float64))
            except Exception:
                pass

            try:
                df0_sorted = df0.sort("match_score", reverse=True)
            except Exception:
                df0_sorted = df0

            try:
                min_score = float(df0_sorted["match_score"].min())
                max_score = float(df0_sorted["match_score"].max())
            except Exception:
                min_score, max_score = 0.0, 100.0

            score_range = st.slider(
                "match_score range",
                min_value=min_score,
                max_value=max_score,
                value=(min_score, max_score),
                format="%.2f"
            )

            filtered = df0_sorted.filter((pl.col("match_score") >= score_range[0]) & (pl.col("match_score") <= score_range[1]))
            try:
                filtered = filtered.sort("match_score", reverse=True)
            except Exception:
                pass
        else:
            df0_sorted = df0
            filtered = df0

        st.markdown(f"**Showing {filtered.height} rows** (filtered from {df0_sorted.height})")
        try:
            st.dataframe(filtered.to_pandas().reset_index(drop=True))
        except Exception:
            st.dataframe(filtered)

        if "match_score" in df0.columns and filtered.height > 0:
            try:
                avg_score = float(filtered["match_score"].mean())
                st.metric("Average match_score (filtered)", f"{avg_score:.1f}")
            except Exception:
                pass

    # --- Dataset 1: posting duration (no additional filtering) ---
    st.header("Matched Job Posting Duration (SOC)")
    if df1.is_empty():
        st.info("No rows returned for dataset 1")
    else:
        # try to sort by median_posting_duration if present
        if "median_posting_duration" in df1.columns:
            try:
                df1_sorted = df1.sort("median_posting_duration", reverse=True)
            except Exception:
                df1_sorted = df1
        else:
            df1_sorted = df1

        # rows counter for dataset 1
        st.markdown(f"**Rows: {df1_sorted.height}**")

        try:
            st.dataframe(df1_sorted.to_pandas())
        except Exception:
            st.dataframe(df1_sorted)


if __name__ == "__main__":
    main()


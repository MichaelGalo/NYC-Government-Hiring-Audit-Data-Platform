## Project Report — NYC Hiring Audit

This document summarizes the pipeline architecture, the final DuckDB/Gold dataset schemas, fuzzy matching approach (library, normalization, thresholds), posting-date handling, project tradeoffs under the 5-day constraint, and a short path to compute numeric summaries for the Tukey Agency.

---

## Overall pipeline architecture and data flow

- Sources
  - NYC government payroll (API)
  - NYC governement job postings (API)
  - Lightcast Data Analytics (Excel Sheet)

- Processing overview
  1. Ingest raw datasets into parquet files.
  2. Run vectorized fuzzy matching flows:
     - `fuzzy_match_salary.py` compares payroll titles → job postings (produces BRONZE/payroll_to_jobs_title_fuzzy_matches.parquet).
     - `fuzzy_match_jobs_durations.py` compares payroll/job titles → Lightcast occupations (produces BRONZE/jobs_to_lightcast_title_fuzzy_matches.parquet).
  3. Post-processing SQL (`sql/cleaned.sql`) selects, renames, computes posting durations and writes two GOLD tables in DuckDB:
     - `GOLD.nyc_salary_matches`
     - `GOLD.nyc_matched_job_posting_duration_SOC`
  4. GOLD datasets are queried by an API (`api/fetch_data.py`) and exposed to the Streamlit app (`streamlit/app.py`).

- Performance & reliability features
  - Vectorized similarity pre-filter (rapidfuzz.process.cdist) + WRatio scoring on candidates
  - Chunking of payroll records and batch parquet writes to limit memory
  - Multi-core workers (rapidfuzz `workers=-1`) and batching to speed execution
  - Simple salary-range filter to reduce false positives
  - Null `post_until` filled with posting_date + 30 days 

---

## Final DuckDB / GOLD schemas

Below are the final columns created by `sql/cleaned.sql`. Types are presented as expected DuckDB/SQL types.

- GOLD.nyc_salary_matches (audit: payroll ↔ job postings)
  - posted_job_title (TEXT) — from `business_title`
  - matched_actual_payroll_title (TEXT) — from `title_description` (payroll)
  - match_score (INTEGER) — fuzzy match score (WRatio, 0–100)
  - posting_min_salary (FLOAT) — `salary_range_from` from posting
  - posting_max_salary (FLOAT) — `salary_range_to` from posting
  - actual_base_salary (FLOAT) — payroll `base_salary`
  - posting_duration_days (INTEGER) — computed: post_until - posting_date
  - actual_gross_paid (FLOAT) — payroll `regular_gross_paid`
  - actual_ot_paid (FLOAT) — payroll `total_ot_paid`
  - actual_other_pay (FLOAT) — payroll `total_other_pay`

- GOLD.nyc_matched_job_posting_duration_SOC (audit: posting durations by SOC)
  - title (TEXT) — job/business title from postings
  - lightcast_matched_occupation (TEXT) — matched Lightcast occupation name
  - total_postings (INTEGER) — "Total Postings (Jan 2024 - Jun 2025)" (kept from source)
  - median_posting_duration (FLOAT/INTEGER) — "Median Posting Duration" (kept from source)

Notes: the SQL used to build these selects and renames columns from the BRONZE intermediate parquet outputs; posting duration is calculated in SQL using strptime/posting_date casts (see `sql/cleaned.sql`).

---

## Fuzzy string matching: library, string generation, scores and thresholds

- Library
  - rapidfuzz - Chosen for speed and familiarity. It supports vectorized calculations, native multi-core workers and returns scores in the 0–100 range.

- How comparison strings are generated
  - Titles are passed through `normalize_title(...)` (see `src/utils.py`): lowercase, remove punctuation, collapse multiple spaces, strip. These normalized strings are what the similarity functions compare.
  - Payroll titles and job posting titles are normalized into two lists and then compared in vectorized chunks.

- Algorithms used
  - Two-step approach:
    1. Token-set pre-filter via `fuzz.token_set_ratio` computed with `process.cdist(...)` to quickly find candidate pairs above a token-set threshold.
    2. Full comparison using `fuzz.WRatio(...)` on the filtered candidates to obtain the final `match_score` (0–100).

- What "Match Ratio" / `match_score` means
  - `WRatio` (Word-aware ratio) in rapidfuzz produces an integer 0–100 where 100 is an exact or near-exact match and lower values indicate less similarity. It blends several heuristics (partial ratios, token set/sort) to handle reordering, abbreviations and partial overlaps.

- Thresholds applied
  - Payroll ↔ job postings (`src/fuzzy_match_salary.py`):
    - token_set_threshold = 85 (pre-filter)
    - score_cutoff (WRatio) = 85 (final keep)
  - Jobs ↔ Lightcast (`src/fuzzy_match_jobs_durations.py`):
    - token_set_threshold = 75
    - score_cutoff (WRatio) = 75

- Rationale
  - Higher thresholds (85) for payroll↔postings reduce false positives when reconciling salaries.
  - Slightly lower thresholds (75) for Lightcast occupation matching allow broader mapping from noisy posting titles to occupations.
  - Token-set prefilter dramatically reduces the number of expensive WRatio calls, enabling the pipeline to run within time constraints.

---

## Posting date filtering and handling for 2024/2025

- Fiscal year filtering (payroll)
  - Payroll rows are filtered by `fiscal_year` between `year_start=2024` and `year_end=2025` in `fuzzy_match_salary.py`. This limits payroll comparisons to the target audit period.

- Job posting date parsing (postings)
  - Posting dates are parsed with Polars into a `%Y-%m-%dT%H:%M:%S` format and rows failing parsing are dropped (to ensure reliable duration calculation).
  - `post_until` nulls are filled by `posting_dates_handler(...)` with `posting_date + 30 days`.
  - Final posting duration is computed in `sql/cleaned.sql` using strptime/posting_date difference.

---

## Key challenges under the 5-day constraint and prioritization

- Main challenges
  1. Selection of Relevant Lightcast Tables
  2. Fuzzy matching performance and ambiguous results
  3. Refactoring from a 1.0 version to 2.1 in order to be better satisfied with my fuzzy matches.

- How these were prioritized:
  - Get an operational minimum viable product function ASAP. Take requisite time to iterate and improve.
  - Use every trick I have for efficiency when moving larger data (lazy frames, polars > pandas, parquets, etc.)
  - Prioritized fuzzy matching and iteration over better data visualization or API improvements.

---

## Summary of findings for the Tukey Agency

- The summary of findings can be easier found in my Streamlit dashboard, but a couple of things to note:
    - Salary | Title Matches: ~700,000, Average match ratio in returned data: 92.7%
    - Lightcast Fuzzy Matches with Salary | Title Matches: 2,356
    - Selected job titles & job posting durations can be found in the API or returned data in streamlit.

---

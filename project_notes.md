Next steps:
- needs new testing after function abstraction
- Talk to Daniel about compute time and results
- Function for converter for ingesting from API, then converting to parquet, then dropping into `data` & `minio`
- Handle Null Date Values for New Fuzzy Matches
- Remove 1.0 Fuzzy Matching Data,
- Update API & Streamlit with new DataSets

Payroll Columns:
- agency_name
- title_description
- base_salary
- pay_basis
- regular_gross_paid
- total_ot_paid
- total_other_pay

Job Posting Columns:
- agency
- business_title
- salary_range_from
- salary_range_to
- posting_date
- post_until (if NULL assume a default of 30 days and mention it)

Lightcast Columns (SOC):
- title
- total postings
- median_posting_duration

Deliverable Datasets:
- job salary match ratios (between job postings and actual payroll data)
- job posting duration dataset (between audited jobs & lightcast data)


Project Notes to Mention in Presentation:
- mention in data architecture that with duck lake that medallion goes out the window. I have raw & cleaned, no staged.
- mention Token_set_ratio: Ignores word order and extra words, focusing on the intersection of words between the two strings. It’s best for matching job titles where titles may have extra descriptors or words in different orders (e.g., "Senior Data Analyst" vs "Data Analyst Senior").
- mention: While ONET is built upon the SOC framework and provides detailed information, SOC itself, as a classification system for all occupations, can be considered more comprehensive in its scope of coverage across the entire labor market, particularly due to its inclusion of residual occupations not explicitly covered by ONET's targeted data collection.
- SOC = Standard Occupational Classification (a hierarchical system used to classify workers into occupational categories for the purpose of collecting, calculating, or disseminating data)


Move from Fuzzy Match 1.0 to 2.0
- Talk about iterations and spending the majority of my time less in data vis but in fuzzy land
- Returning around 600 results, then further matched only to 2 felt off after feedback (I had a MVP, but I wanted to iterate on it and deep dive -- as I thought it was the primary learning objective of this project. That and creating our own architecture)
- I instituted a new fuzzy_matching paradigm that used vectorization, Token Set Matching Pre-Filter, WRatio and specifying how many CPU cores I would allow for parallel processing among other things to return:
    - Run time Total for No Limit: 2:23:19
    - Total Returned Results for No Limit | 8,737,221
    - 20+ parquet temp files that ultimately compile into a single monster dataset
    - That 38.4B computations that were required (after lots of efficieny steps)

- After that, it was time to compare that new dataset to the lightcast data using a similar approach albeit less computations.
    - Run time Total for No Limit: 1:49
    - Total Returned Results for No Limit | 30,714
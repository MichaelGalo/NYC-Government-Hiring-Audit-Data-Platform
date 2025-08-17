Next steps:
- write fuzzy matcher where I have 2 dataframes (payroll and job postings)
- write silver tier sql scripts

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

Lightcast Columns useful:
- Lightcast Job Title
- Lightcast Total Postings
- Lightcast Median Posting Duration

Deliverable Datasets:
- job posting duration dataset
- job salary match ratios

Fuzzy Match Process:
- Token_set_ratio: Ignores word order and extra words, focusing on the intersection of words between the two strings. It’s best for matching job titles where titles may have extra descriptors or words in different orders (e.g., "Senior Data Analyst" vs "Data Analyst Senior").
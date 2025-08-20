Next steps:
- apparently write another fuzzy match between the audited jobs and the lightcast jobs with a 75 ratio (business titles only) use all tables and literally give them 4 different dataset results
- I need to go back and see if I am only returning the first fuzzy match or ALL fuzzy matches

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

Lightcast Executive Summary:
- title
- total postings
- median_posting_duration

Lightcast Top Jobs Columns:
- 

Deliverable Datasets:
- job salary match ratios (between job postings and actual payroll data)
- job posting duration dataset (between audited jobs & lightcast data)


Fuzzy Match Process:
-- maybe offer some other reports (that they didn't ask for, break lightcast for 4 tables)




GRAB ALL occupation data (all tables, deliniated by the table name headers (top, SOC, etc.))


Project Notes to Mention in Presentation:
- mention in data architecture that with duck lake that medallion goes out the window. I have raw & cleaned, no staged.
- mention Token_set_ratio: Ignores word order and extra words, focusing on the intersection of words between the two strings. It’s best for matching job titles where titles may have extra descriptors or words in different orders (e.g., "Senior Data Analyst" vs "Data Analyst Senior").
- mention: While ONET is built upon the SOC framework and provides detailed information, SOC itself, as a classification system for all occupations, can be considered more comprehensive in its scope of coverage across the entire labor market, particularly due to its inclusion of residual occupations not explicitly covered by ONET's targeted data collection.
- SOC = Standard Occupational Classification (a hierarchical system used to classify workers into occupational categories for the purpose of collecting, calculating, or disseminating data)


Move from Fuzzy Match 1.0 to 2.0
- Returning around 600 results, then further matched only to 2 felt off after feedback
- I instituted a new fuzzy_matching paradigm that used vectorization, Token Set Matching Pre-Filter, WRatio among other things to return:
    - Run time Total for No Limit: 2:23:19
    - Total Returned Results for No Limit | No Distinct: 8,737,221

- After that, I decided it was worth setting limits as many of the results I was returning were duplicate titles/payroll titles (though these looked to match actual different jobs based on their variance in pay).

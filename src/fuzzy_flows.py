
from prefect.client.schemas.schedules import CronSchedule
from prefect import flow
from fuzzy_match_salary import fuzzy_match_salary
from fuzzy_match_jobs_durations import fuzzy_match_jobs_duration

# @flow(name="fuzzy_match")
def fuzzy_match():
    fuzzy_match_salary()
    fuzzy_match_jobs_duration()

# if __name__ == "__main__":
#     fuzzy_match.serve(
#         name="Data_Ingestion",
#         schedule=CronSchedule(
#             cron="0 1 * * 0",
#             timezone="UTC"
#         ), # sundays at 1 am
#         tags=["data_ingestion", "weekly"]
#     )

fuzzy_match()
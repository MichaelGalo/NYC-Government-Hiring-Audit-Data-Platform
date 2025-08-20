
from prefect.client.schemas.schedules import CronSchedule
from prefect import flow
from fuzzy_match_salary import fuzzy_match_payroll_to_jobs_vectorized
from fuzzy_match_jobs_durations import fuzzy_match_jobs_to_lightcast_vectorized

@flow(name="fuzzy_match")
def fuzzy_match():
    fuzzy_match_payroll_to_jobs_vectorized()
    fuzzy_match_jobs_to_lightcast_vectorized()

if __name__ == "__main__":
    fuzzy_match.serve(
        name="Fuzzy_Match",
        schedule=CronSchedule(
            cron="0 1 * * 0",
            timezone="UTC"
        ), # sundays at 1 am
        tags=["fuzzy_matching", "weekly"]
    )
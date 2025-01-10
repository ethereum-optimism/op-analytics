from dagster import job, op, ScheduleDefinition


@op
def stablecoins():
    print("hello")


@job
def defillama_job():
    stablecoins()


defillama_schedule = ScheduleDefinition(
    job=defillama_job,
    cron_schedule="0 3 * * *",  # Runs at 3 AM daily
    execution_timezone="UTC",
)

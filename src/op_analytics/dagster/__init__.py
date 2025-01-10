from dagster import Definitions

import op_analytics.dagster.jobs as jobs


defs = Definitions(
    jobs=[jobs.defillama.defillama_job],
    schedules=[jobs.defillama.defillama_schedule],
)

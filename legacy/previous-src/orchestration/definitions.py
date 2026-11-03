from dagster import Definitions
from src.orchestration.assets import all_assets
from src.orchestration.jobs import all_jobs
from src.orchestration.schedules import all_schedules

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
)

import os
from dagster import Definitions, load_assets_from_modules
from .assets import city_revenue, city_expenses
from .jobs import revenue_update_job, expense_update_job
from .resources import RESOURCES_LOCAL, RESOURCES_STAGING, RESOURCES_PROD
from .schedules import revenue_update_schedule, expense_update_schedule
from .sensors import discord_on_run_failure

city_revenue_assets = load_assets_from_modules([city_revenue, city_expenses])

all_assets = [*city_revenue_assets]
all_jobs = [revenue_update_job, expense_update_job]
all_schedule = [revenue_update_schedule, expense_update_schedule]
all_sensors = [discord_on_run_failure]
resources_by_deployment_name = {
    "prod": RESOURCES_PROD,
    "staging": RESOURCES_STAGING,
    "local": RESOURCES_LOCAL,
}


deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedule,
    sensors=all_sensors,
    resources=resources_by_deployment_name[deployment_name],
)

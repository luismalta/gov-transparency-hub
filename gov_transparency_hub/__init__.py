import os

from dagster import Definitions, load_assets_from_modules, multiprocess_executor

from .assets import dbt_curated, df_reports, df_resources, raw_reports, raw_resources
from .resources import RESOURCES_LOCAL, RESOURCES_PROD, RESOURCES_STAGING
from .sensors import discord_on_run_failure

all_assets = load_assets_from_modules(
    [df_resources, df_reports, raw_reports, raw_resources, dbt_curated]
)
all_sensors = [discord_on_run_failure]
resources_by_deployment_name = {
    "prod": RESOURCES_PROD,
    "staging": RESOURCES_STAGING,
    "local": RESOURCES_LOCAL,
}

deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")

defs = Definitions(
    assets=all_assets,
    # Steps are mostly I/O bound (portal API, Databricks), so they run in
    # parallel; the cap bounds processes and memory during backfills.
    executor=multiprocess_executor.configured({"max_concurrent": 4}),
    sensors=all_sensors,
    resources=resources_by_deployment_name[deployment_name],
)

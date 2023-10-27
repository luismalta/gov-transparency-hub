import os
from dagster import Definitions
from .assets import city_revenue_assets
from .resources import RESOURCES_LOCAL, RESOURCES_STAGING, RESOURCES_PROD
# from .jobs import city_revenue_job

all_assets = [*city_revenue_assets]
resources_by_deployment_name = {
    "prod": RESOURCES_PROD,
    "staging": RESOURCES_STAGING,
    "local": RESOURCES_LOCAL,
}


deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")

defs = Definitions(
    assets=all_assets,
    # jobs=[city_revenue_job],
    resources=resources_by_deployment_name[deployment_name],
)
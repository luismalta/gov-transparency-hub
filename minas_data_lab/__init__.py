import os
from dagster import Definitions, load_assets_from_modules
from .assets import city_revenue
from .resources import RESOURCES_LOCAL, RESOURCES_STAGING, RESOURCES_PROD

city_revenue_assets = load_assets_from_modules([city_revenue])

all_assets = [*city_revenue_assets]
resources_by_deployment_name = {
    "prod": RESOURCES_PROD,
    "staging": RESOURCES_STAGING,
    "local": RESOURCES_LOCAL,
}


deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")

defs = Definitions(
    assets=all_assets,
    resources=resources_by_deployment_name[deployment_name],
)
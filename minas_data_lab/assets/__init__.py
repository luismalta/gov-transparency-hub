from dagster import load_assets_from_package_module

from . import city_revenue

REVENUE = 'revenue'

city_revenue_assets = load_assets_from_package_module(package_module=city_revenue, group_name=REVENUE)
from dagster import define_asset_job
from .assets.city_revenue.city_revenue import CityRevenueConfig


# @job
# def city_revenue():
#     city_revenue()

# job_result = greeting_job.execute_in_process(
#     run_config=RunConfig({"print_greeting": MyOpConfig(person_name="Alice")})
# )

city_revenue_job = define_asset_job(
    name="city_revenue",
    selection="city_revenue",
    # config={"city_revenue": CityRevenueConfig(city="Tiradentes")}
)

# asset_result = materialize(
#     [city_revenue, city_revenue_cleaned],
#     run_config=RunConfig({"city_revenue": CityRevenueConfig(city="Tiradentes")}),
# )

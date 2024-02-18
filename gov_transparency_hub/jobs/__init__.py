from dagster import AssetSelection, define_asset_job
from ..partitions import daily_city_partition


revenue_update_job = define_asset_job(
    name="revenue_update_job",
    partitions_def=daily_city_partition,
    selection=AssetSelection.groups("revenue"),
)

expense_update_job = define_asset_job(
    name="expense_update_job",
    partitions_def=daily_city_partition,
    selection=AssetSelection.groups("expense"),
)

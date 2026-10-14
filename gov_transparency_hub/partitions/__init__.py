from dagster import (
    DailyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    PartitionsDefinition,
    StaticPartitionsDefinition,
    TimeWindowPartitionsDefinition,
)

from ..assets import constants

start_date = constants.START_DATE

daily_partition = DailyPartitionsDefinition(
    start_date=start_date,
)

city_partition = StaticPartitionsDefinition(constants.CITY_LIST)


daily_city_partition = MultiPartitionsDefinition(
    {
        "date": daily_partition,
        "city": city_partition,
    }
)

monthly_city_partition = MultiPartitionsDefinition(
    {
        # end_offset=1: the open month/year is refreshed while it is in progress.
        "month": MonthlyPartitionsDefinition(
            start_date=start_date[:7], fmt="%Y-%m", end_offset=1
        ),
        "city": city_partition,
    }
)

yearly_city_partition = MultiPartitionsDefinition(
    {
        "year": TimeWindowPartitionsDefinition(
            cron_schedule="0 0 1 1 *", start=start_date[:4], fmt="%Y", end_offset=1
        ),
        "city": city_partition,
    }
)


def partitions_def_for(param_names: tuple[str, ...]) -> PartitionsDefinition:
    """Partition an endpoint by the finest period its parameters filter on."""
    if "data_do_empenho_inicial" in param_names:
        return daily_city_partition
    if "mes_de_criacao" in param_names:
        return monthly_city_partition
    if "exercicio" in param_names or "exercicio_de_criacao" in param_names:
        return yearly_city_partition
    return city_partition


def partition_scope(
    partitions_def: PartitionsDefinition, partition_key: str
) -> tuple[str, str | None]:
    """Split a partition key into (city, period); period is None for city-only."""
    if not isinstance(partitions_def, MultiPartitionsDefinition):
        return partition_key, None
    dimensions = dict(
        partitions_def.get_partition_key_from_str(partition_key).keys_by_dimension
    )
    city = dimensions.pop("city")
    (period,) = dimensions.values()
    return city, period

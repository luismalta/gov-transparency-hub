from dagster import (
    DailyPartitionsDefinition,
    StaticPartitionsDefinition,
    MultiPartitionsDefinition
)
from ..assets import constants


start_date = constants.START_DATE

daily_partition = DailyPartitionsDefinition(
  start_date=start_date,
)

city_partition = StaticPartitionsDefinition(
    constants.CITY_LIST
)


daily_city_partition = MultiPartitionsDefinition(
    {
        "date": daily_partition,
        "city": city_partition,
    }
)
import pandas as pd
from datetime import datetime
from io import StringIO, BytesIO
from sqlalchemy.exc import NoResultFound
from dagster import Output, asset, MetadataValue

from gov_transparency_hub.resources import PostgresResource, S3Resource
from gov_transparency_hub.resources.PortalTransparenciaScrapper import (
    PortalTransparenciaScrapper,
)
from gov_transparency_hub.partitions import daily_city_partition
from gov_transparency_hub.assets.utils import format_object_name
from gov_transparency_hub.assets.constants import CITY_REVENUE_COLUMNS_RENAME

SELECT_LAST_REVENUE_DATE_FROM_MONTH = """
    select date
    from revenue
    where to_char(date, 'YYYY-MM') = '{}' and city = '{}'
    order by date desc
    limit 1
"""


@asset(partitions_def=daily_city_partition, group_name="revenue")
def city_revenue_file(context, s3_resource: S3Resource):
    """
    Raw report dowloaded from Portal da Transparencia
    """
    dimensions = context.partition_key.keys_by_dimension
    city_name = dimensions.get("city")

    bucket_name = city_name
    partition_date_str = dimensions.get("date")
    year_to_fetch = partition_date_str[:4]
    month_to_fetch = int(partition_date_str[5:7])

    payload = [
        f"INT_EXR={year_to_fetch}",
        "CHAR_ID_EMP=1",
        "LG_ALT_PAG=S",
        f"INT_MES_INI={month_to_fetch}",
        f"INT_MES_FIM={month_to_fetch}",
        "URL=Tempo_Real_Receitas",
    ]
    scrapper = PortalTransparenciaScrapper(city_name)
    report = scrapper.get_report("Tempo_Real_Receitas", "csv", payload)

    city_revenue_df = pd.read_csv(StringIO(report), index_col=False)

    object_name = format_object_name("revenue", bucket_name, partition_date_str)
    s3_resource.upload_object(bucket_name, object_name, city_revenue_df)


@asset(
    deps=["city_revenue_file"], partitions_def=daily_city_partition, group_name="revenue"
)
def city_revenue(context, s3_resource: S3Resource, postgres_resource: PostgresResource):
    """
    Raw city revenue dataset, loaded into Postgres database
    """
    dimensions = context.partition_key.keys_by_dimension
    city_name = dimensions.get("city")
    partition_date_str = dimensions.get("date")

    bucket = city_name
    object_name = f"revenue/{city_name}-revenue-{partition_date_str}"
    city_revenue_df = s3_resource.get_object(bucket, object_name)

    city_revenue_df.query(
        'not Data.str.contains("TOTAL")', engine="python", inplace=True
    )
    city_revenue_df.query(
        'not Data.str.contains("Total do Dia")', engine="python", inplace=True
    )
    city_revenue_df.rename(columns=CITY_REVENUE_COLUMNS_RENAME, inplace=True)

    city_revenue_df["date"] = pd.to_datetime(city_revenue_df["date"], format="%d/%m/%Y")
    city_revenue_df["city"] = city_name

    query_cursor = postgres_resource.execute_query(
        SELECT_LAST_REVENUE_DATE_FROM_MONTH.format(partition_date_str[:7], city_name)
    )

    last_revenue = False
    try:
        last_revenue = query_cursor.one()
    except NoResultFound:
        context.log.info("No record of previous revenue found in database")
    if last_revenue:
        last_revenue_date = last_revenue[0]
        city_revenue_df.query(f'date > "{last_revenue_date}"', inplace=True)

    postgres_resource.save_dataframe("revenue", city_revenue_df)

    return Output(
        city_revenue_df,
        metadata={
            "Count": len(city_revenue_df),
            "preview": MetadataValue.md(city_revenue_df.head().to_markdown()),
        },
    )

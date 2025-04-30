import dlt
from typing import Any, Optional
from collections.abc import Mapping
from dagster import Output, asset, MetadataValue
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator

from gov_transparency_hub.resources import PostgresResource, S3Resource, dbt_project
from gov_transparency_hub.resources.PortalTransparenciaScrapper import (
    PortalTransparenciaScrapper,
)
from gov_transparency_hub.partitions import daily_city_partition
from gov_transparency_hub.assets.utils import format_object_name, create_surrogate_key

DELETE_REVENUE_FROM_CURRENT_MONTH = """
    DELETE FROM raw.revenue_details
    WHERE municipio = '{}' 
    AND Data like '%/{}/{}'
"""


@asset(partitions_def=daily_city_partition, group_name="revenue", kinds=["s3"])
def revenue_html_report(context, s3_resource: S3Resource):
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
    revenue_html_report = scrapper.get_report("Tempo_Real_Receitas", "html", payload)

    object_name = format_object_name("revenue_html", bucket_name, partition_date_str)
    s3_resource.upload_html(bucket_name, object_name, revenue_html_report)


@asset(
    deps=["revenue_html_report"],
    partitions_def=daily_city_partition,
    group_name="revenue",
    kinds=["s3"],
)
def extract_revenue_deatils_df(
    context, s3_resource: S3Resource, postgres_resource: PostgresResource
):
    """
    Raw city revenue dataset, loaded into Postgres database
    """
    dimensions = context.partition_key.keys_by_dimension
    city_name = dimensions.get("city")
    partition_date_str = dimensions.get("date")

    bucket_name = city_name
    object_name = f"revenue_html/{city_name}-revenue_html-{partition_date_str}"
    revenue_html_report = s3_resource.download_html(bucket_name, object_name)

    scrapper = PortalTransparenciaScrapper(city_name)
    revenue_df = scrapper.extract_revenue_deatils(revenue_html_report)

    object_name = format_object_name(
        "revenue_details_df", bucket_name, partition_date_str
    )
    s3_resource.upload_object(bucket_name, object_name, revenue_df)

    return Output(
        revenue_df,
        metadata={
            "Count": len(revenue_df),
            "preview": MetadataValue.md(revenue_df.head().to_markdown()),
        },
    )


@asset(
    deps=["extract_revenue_deatils_df"],
    partitions_def=daily_city_partition,
    group_name="revenue",
    kinds=["s3", "dlt", "postgres"],
)
def load_raw_revenue_details(
    context, s3_resource: S3Resource, postgres_resource: PostgresResource
):
    """
    Create the raw table in database combining all the revenue details DataFrames
    """
    dimensions = context.partition_key.keys_by_dimension
    city_name = dimensions.get("city")
    partition_date_str = dimensions.get("date")
    bucket_name = city_name

    object_name = format_object_name(
        "revenue_details_df", bucket_name, partition_date_str
    )

    revenue_details_df = s3_resource.get_object(bucket_name, object_name)

    if revenue_details_df.empty:
        return

    revenue_details_df["municipio"] = city_name

    year_to_fetch = partition_date_str[:4]
    month_to_fetch = partition_date_str[5:7]

    revenue_details_df["surrogate_key"] = create_surrogate_key(
        revenue_details_df,
        hash_columns=revenue_details_df.columns.tolist(),
        prefix_columns=["municipio"]
    )

    try:
        postgres_resource.execute_query(
            DELETE_REVENUE_FROM_CURRENT_MONTH.format(
                city_name, month_to_fetch, year_to_fetch
            )
        )
    except Exception:
        pass

    pipeline = dlt.pipeline(
        pipeline_name="load_raw_revenue_details",
        destination=dlt.destinations.postgres(postgres_resource.get_conn_str()),
        dataset_name="raw",
    )

    pipeline.run(
        data=revenue_details_df,
        table_name="revenue_details",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        primary_key=["surrogate_key"],
    )

    )


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        return "revenue"


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    select="revenue_*",
)
def dbt_models_revenue(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

import pandas as pd
import base64
import dlt
from typing import Any, Optional
from collections.abc import Mapping
from datetime import datetime
from dagster import asset
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator

from gov_transparency_hub.resources import PostgresResource, S3Resource, dbt_project
from gov_transparency_hub.resources.PortalTransparenciaScrapper import (
    PortalTransparenciaScrapper,
)
from gov_transparency_hub.partitions import daily_city_partition
from gov_transparency_hub.assets.utils import format_object_name, create_surrogate_key


@asset(partitions_def=daily_city_partition, group_name="expense", kinds=["s3"])
def expense_html_report(context, s3_resource: S3Resource):
    """
    Raw report dowloaded from Portal da Transparencia
    """
    dimensions = context.partition_key.keys_by_dimension
    city_name = dimensions.get("city")
    bucket_name = city_name

    partition_date_str = dimensions.get("date")
    year_to_fetch = partition_date_str[:4]
    day_to_fetch = datetime.strptime(partition_date_str, "%Y-%m-%d").strftime(
        "%d/%m/%Y"
    )

    payload = [
        f"INT_EXR={year_to_fetch}",
        "CHAR_ID_EMP=1",
        f"D_DESP_DE={day_to_fetch}",
        f"D_DESP_ATE={day_to_fetch}",
        "URL=Tempo_Real_Despesa",
    ]
    scrapper = PortalTransparenciaScrapper(city_name)

    report_html = scrapper.get_report("Tempo_Real_Despesa", "html", payload)

    object_name = format_object_name(
        "expense_html_report", bucket_name, partition_date_str
    )

    s3_resource.upload_html(bucket_name, object_name, report_html)


@asset(
    deps=["expense_html_report"],
    partitions_def=daily_city_partition,
    group_name="expense",
    kinds=["s3"],
)
def expense_details_html(context, s3_resource: S3Resource):
    """
    Download and save expense details html report
    """
    dimensions = context.partition_key.keys_by_dimension
    city_name = dimensions.get("city")
    partition_date_str = dimensions.get("date")
    bucket_name = city_name

    object_name = format_object_name(
        "expense_html_report", bucket_name, partition_date_str
    )
    expense_html_report = s3_resource.download_html(bucket_name, object_name)

    scrapper = PortalTransparenciaScrapper(city_name)

    expenses = scrapper.get_expense_details_pages(expense_html_report)

    for expense in expenses:
        base64_expense_url = base64.b64encode(expense["url"].encode()).decode()
        object_name = f"expense_details_html/{partition_date_str}/{base64_expense_url}"
        s3_resource.upload_html(bucket_name, object_name, expense["html_content"])


@asset(
    deps=["expense_details_html"],
    partitions_def=daily_city_partition,
    group_name="expense",
    kinds=["s3"],
)
def extract_expense_details_df(context, s3_resource: S3Resource):
    """
    Extract and save expense details from html in a DataFrame
    """
    dimensions = context.partition_key.keys_by_dimension
    city_name = dimensions.get("city")
    partition_date_str = dimensions.get("date")
    bucket_name = city_name

    object_name = f"expense_details_html/{partition_date_str}"
    expenses_details_html = s3_resource.download_folder_contents(
        bucket_name, object_name
    )

    scrapper = PortalTransparenciaScrapper(city_name)
    extracted_expenses_details = scrapper.extract_expenses_details(
        expenses_details_html
    )

    expenses_df = pd.DataFrame(extracted_expenses_details)

    object_name = format_object_name(
        "expense_details_df", bucket_name, partition_date_str
    )
    s3_resource.upload_object(bucket_name, object_name, expenses_df)


@asset(
    deps=["extract_expense_details_df"],
    partitions_def=daily_city_partition,
    group_name="expense",
    kinds=["s3", "dlt", "postgres"],
)
def load_raw_expense_details(
    context, s3_resource: S3Resource, postgres_resource: PostgresResource
):
    """
    Create the raw table in database combining all the expense details DataFrames
    """
    dimensions = context.partition_key.keys_by_dimension
    city_name = dimensions.get("city")
    partition_date_str = dimensions.get("date")
    bucket_name = city_name

    object_name = format_object_name(
        "expense_details_df", bucket_name, partition_date_str
    )

    expense_details_df = s3_resource.get_object(bucket_name, object_name)

    if not expense_details_df.empty:

        expense_details_df["municipio"] = city_name

        expense_details_df["surrogate_key"] = create_surrogate_key(
            expense_details_df,
            hash_columns=expense_details_df.columns.tolist(),
            prefix_columns=["numero", "ano", "municipio"]
        )


        pipeline = dlt.pipeline(
            pipeline_name="load_raw_expense_details",
            destination=dlt.destinations.postgres(postgres_resource.get_conn_str()),
            dataset_name="raw",
        )

        pipeline.run(
            data=expense_details_df,
            table_name="expense_details",
            write_disposition={"disposition": "merge", "strategy": "upsert"},
            primary_key=["surrogate_key"],
        )
    )


@asset(
    deps=["expense_details_html"],
    partitions_def=daily_city_partition,
    group_name="expense",
    kinds=["s3"],
)
def extract_expense_itens_df(context, s3_resource: S3Resource):
    """
    Extract and save expense itens from html in a DataFrame
    """
    dimensions = context.partition_key.keys_by_dimension
    city_name = dimensions.get("city")
    partition_date_str = dimensions.get("date")
    bucket_name = city_name

    object_name = f"expense_details_html/{partition_date_str}"
    expenses_details_html = s3_resource.download_folder_contents(
        bucket_name, object_name
    )

    scrapper = PortalTransparenciaScrapper(city_name)
    extracted_expenses_itens = scrapper.extract_expense_itens(expenses_details_html)

    itens_df = pd.DataFrame(extracted_expenses_itens)

    object_name = format_object_name(
        "expense_itens_df", bucket_name, partition_date_str
    )
    s3_resource.upload_object(bucket_name, object_name, itens_df)


@asset(
    deps=["extract_expense_itens_df"],
    partitions_def=daily_city_partition,
    group_name="expense",
    kinds=["s3", "dlt", "postgres"],
)
def load_raw_expense_itens(
    context, s3_resource: S3Resource, postgres_resource: PostgresResource
):
    """
    Create the raw table in database combining all the expense itens DataFrames
    """
    dimensions = context.partition_key.keys_by_dimension
    city_name = dimensions.get("city")
    partition_date_str = dimensions.get("date")
    bucket_name = city_name

    object_name = format_object_name(
        "expense_itens_df", bucket_name, partition_date_str
    )

    expense_itens_df = s3_resource.get_object(bucket_name, object_name)

    if not expense_itens_df.empty:

        expense_itens_df["municipio"] = city_name

        expense_itens_df["surrogate_key"] = create_surrogate_key(
            expense_itens_df,
            hash_columns=expense_itens_df.columns.tolist(),
            prefix_columns=["item", "expense_number", "expense_year", "municipio"]
        )

        pipeline = dlt.pipeline(
            pipeline_name="load_raw_expense_itens",
            destination=dlt.destinations.postgres(postgres_resource.get_conn_str()),
            dataset_name="raw",
        )

        pipeline.run(
            expense_itens_df,
            table_name="expense_itens",
            write_disposition={"disposition": "merge", "strategy": "upsert"},
            primary_key=["surrogate_key"],
        )
        expense_itens_df,
        table_name="expense_itens",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        primary_key=["item", "expense_number", "expense_year", "municipio"],
    )


@asset(
    deps=["expense_details_html"],
    partitions_def=daily_city_partition,
    group_name="expense",
    kinds=["s3"],
)
def extract_expense_invoices_df(context, s3_resource: S3Resource):
    """
    Extract and save expense invoices from html in a DataFrame
    """
    dimensions = context.partition_key.keys_by_dimension
    city_name = dimensions.get("city")
    partition_date_str = dimensions.get("date")
    bucket_name = city_name

    object_name = f"expense_details_html/{partition_date_str}"
    expenses_details_html = s3_resource.download_folder_contents(
        bucket_name, object_name
    )

    scrapper = PortalTransparenciaScrapper(city_name)
    extracted_expenses_invoices = scrapper.extract_expense_invoice(
        expenses_details_html
    )

    invoices_df = pd.DataFrame(extracted_expenses_invoices)

    object_name = format_object_name(
        "expense_invoices_df", bucket_name, partition_date_str
    )
    s3_resource.upload_object(bucket_name, object_name, invoices_df)


@asset(
    deps=["extract_expense_invoices_df"],
    partitions_def=daily_city_partition,
    group_name="expense",
    kinds=["s3", "dlt", "postgres"],
)
def load_raw_expense_invoices(
    context, s3_resource: S3Resource, postgres_resource: PostgresResource
):
    """
    Create the raw table in database combining all the expense invoices DataFrames
    """
    dimensions = context.partition_key.keys_by_dimension
    city_name = dimensions.get("city")
    partition_date_str = dimensions.get("date")
    bucket_name = city_name

    object_name = format_object_name(
        "expense_invoices_df", bucket_name, partition_date_str
    )

    expense_invoices_df = s3_resource.get_object(bucket_name, object_name)

    if not expense_invoices_df.empty:

        expense_invoices_df["municipio"] = city_name

        expense_invoices_df["surrogate_key"] = create_surrogate_key(
            expense_invoices_df,
            hash_columns=expense_invoices_df.columns.tolist(),
            prefix_columns=["municipio"]
        )

        pipeline = dlt.pipeline(
            pipeline_name="load_raw_expense_invoices",
            destination=dlt.destinations.postgres(postgres_resource.get_conn_str()),
            dataset_name="raw",
        )

        pipeline.run(
            data=expense_invoices_df,
            table_name="expense_invoices",
            write_disposition={"disposition": "merge", "strategy": "upsert"},
            primary_key=["surrogate_key"],
        )

    )


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        return "expense"


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    select="expense_*",
)
def dbt_models(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

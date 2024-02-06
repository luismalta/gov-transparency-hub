import pandas as pd
from datetime import datetime
from io import StringIO, BytesIO
from sqlalchemy.exc import NoResultFound
from dagster import Output, asset, MetadataValue

from gov_transparency_hub.resources import PostgresResource, S3Resource
from gov_transparency_hub.resources.PortalTransparenciaScrapper import PortalTransparenciaScrapper
from gov_transparency_hub.partitions import daily_city_partition
from gov_transparency_hub.assets.constants import CITY_EXPENSE_COLUMNS_RENAME
from gov_transparency_hub.assets.utils import format_object_name


@asset(
    partitions_def=daily_city_partition,
    group_name="expense"
)
def city_expense_file(context, s3_resource: S3Resource):
    """
        Raw report dowloaded from Portal da Transparencia
    """
    dimensions = context.partition_key.keys_by_dimension
    city_name = dimensions.get('city')
    bucket_name = city_name

    partition_date_str = dimensions.get('date')
    year_to_fetch = partition_date_str[:4]
    day_to_fetch = datetime.strptime(partition_date_str, '%Y-%m-%d').strftime('%d/%m/%Y')

    payload = [f'INT_EXR={year_to_fetch}', 'CHAR_ID_EMP=1', f'D_DESP_DE={day_to_fetch}', f'D_DESP_ATE={day_to_fetch}', 'URL=Tempo_Real_Despesa']
    scrapper = PortalTransparenciaScrapper(city_name)
    report = scrapper.get_report('Tempo_Real_Despesa', 'csv', payload)

    city_revenue_df = pd.read_csv(StringIO(report), index_col=False)

    object_name = format_object_name('summarized_expense', bucket_name, partition_date_str)
    s3_resource.upload_object(bucket_name, object_name, city_revenue_df)


@asset(
    deps=["city_expense_file"],
    partitions_def=daily_city_partition,
    group_name="expense"
)
def city_expense(context, s3_resource: S3Resource, postgres_resource: PostgresResource):
    """
        Raw city revenue dataset, loaded into Postgres database
    """
    dimensions = context.partition_key.keys_by_dimension
    city_name = dimensions.get('city')
    partition_date_str = dimensions.get('date')
    bucket_name = city_name

    object_name = format_object_name('summarized_expense', bucket_name, partition_date_str)
    city_expense = s3_resource.get_object(bucket_name, object_name)

    city_expense.dropna(subset=['Credor'], how='all', inplace=True)
    city_expense.query('not Credor.str.contains("Total")', engine='python', inplace=True)
    city_expense.query('not Credor.str.contains("Clique na lupa para mais informações")', engine='python', inplace=True)

    scrapper = PortalTransparenciaScrapper(city_name)

    detailed_expenses_list = []
    for index, row in city_expense.iterrows():
        expense_number, expense_year = row['Empenho'].split(" / ")
        expense_number.replace("-", "")
        detailed_expenses = scrapper.get_detailed_expense(expense_number, expense_year)
        detailed_expenses_list.append(detailed_expenses)
    
    detailed_expenses_df = pd.DataFrame.from_dict(detailed_expenses_list)
    if detailed_expenses_list:

        detailed_expenses_df.rename(columns=CITY_EXPENSE_COLUMNS_RENAME, inplace=True)

        detailed_expenses_df['effort_date'] = pd.to_datetime(detailed_expenses_df['effort_date'], format="%d/%m/%Y")
        detailed_expenses_df['settlement_date'] = pd.to_datetime(detailed_expenses_df['settlement_date'], format="%d/%m/%Y")
        detailed_expenses_df['payment_date'] = pd.to_datetime(detailed_expenses_df['payment_date'], format="%d/%m/%Y")

        detailed_expenses_df['value'] = detailed_expenses_df['value'].replace('\.', '',regex=True)
        detailed_expenses_df['value'] = detailed_expenses_df['value'].replace(',', '.', regex=True)
        detailed_expenses_df.astype({'value': 'float'})

        detailed_expenses_df['city'] = city_name

        object_name = format_object_name('expense', bucket_name, partition_date_str)
        s3_resource.upload_object(bucket_name, object_name, detailed_expenses_df)

        postgres_resource.save_dataframe('expense', detailed_expenses_df)

    return Output(
        detailed_expenses_df,
        metadata={
            "Count": len(detailed_expenses_df),
            "preview": MetadataValue.md(detailed_expenses_df.head().to_markdown()),
        }
    )
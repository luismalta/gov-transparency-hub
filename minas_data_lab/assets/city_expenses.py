import pandas as pd
from datetime import datetime
from io import StringIO, BytesIO
from sqlalchemy.exc import NoResultFound
from dagster import Output, asset, MetadataValue

from minas_data_lab.resources import PostgresResource, S3Resource
from minas_data_lab.resources.PortalTransparenciaScrapper import PortalTransparenciaScrapper
from minas_data_lab.partitions import daily_city_partition

SELECT_LAST_REVENUE_DATE_FROM_MONTH = """
    select date
    from expense
    where to_char(date, 'YYYY-MM') = '{}'
    order by date desc
    limit 1
"""


@asset(
    partitions_def=daily_city_partition
)
def city_expense_file(context, s3_resource: S3Resource):
    """
        Raw report dowloaded from Portal da Transparencia
    """
    dimensions = context.partition_key.keys_by_dimension
    city_name = dimensions.get('city')

    partition_date_str = dimensions.get('date')
    year_to_fetch = partition_date_str[:4]
    day_to_fetch = datetime.strptime(partition_date_str, '%Y-%m-%d').strftime('%d/%m/%Y')

    payload = [f'INT_EXR={year_to_fetch}', 'CHAR_ID_EMP=1', f'D_DESP_DE={day_to_fetch}', f'D_DESP_ATE={day_to_fetch}', 'URL=Tempo_Real_Despesa']
    scrapper = PortalTransparenciaScrapper(city_name)
    report = scrapper.get_report('Tempo_Real_Despesa', 'csv', payload)

    city_revenue_df = pd.read_csv(StringIO(report), index_col=False)

    bucket = city_name
    object_name = f"expense/{city_name}-expense-{partition_date_str}"
    s3_resource.upload_object(bucket, object_name, city_revenue_df)

@asset(
    deps=["city_expense_file"],
    partitions_def=daily_city_partition
)
def city_expense(context, s3_resource: S3Resource, postgres_resource: PostgresResource):
    """
        Raw city revenue dataset, loaded into Postgres database
    """
    dimensions = context.partition_key.keys_by_dimension
    city_name = dimensions.get('city')
    partition_date_str = dimensions.get('date')

    bucket = city_name
    object_name = f"expense/{city_name}-expense-{partition_date_str}"
    city_expense = s3_resource.get_object(bucket, object_name)


    columns_rename = {
        'Credor': 'creditor',
        'Empenho': 'effort',
        'Tipo': 'type',
        'Processo Licitatório': 'bidding_process',
        'Data Empenho': 'effort_date',
        'Data Liquidação': 'settlement_date',
        'Data Pagamento': 'payment_date',
        'Valor Empenhado': 'effort_value',
        'Valor Liquidado': 'settlement_value',
        'Valor Pago': 'paid_value',
    }

    city_expense.dropna(subset=['Credor'], how='all', inplace=True)
    city_expense.query('not Credor.str.contains("Total")', engine='python', inplace=True)
    city_expense.query('not Credor.str.contains("Clique na lupa para mais informações")', engine='python', inplace=True)
    city_expense.rename(columns=columns_rename, inplace=True)
    city_expense.drop('Unnamed: 10', axis=1, inplace=True)

    city_expense['effort_date'] = pd.to_datetime(city_expense['effort_date'], format="%d/%m/%Y")
    city_expense['settlement_date'] = pd.to_datetime(city_expense['settlement_date'], format="%d/%m/%Y")
    city_expense['payment_date'] = pd.to_datetime(city_expense['payment_date'], format="%d/%m/%Y")

    scrapper = PortalTransparenciaScrapper(city_name)

    detailed_expenses_list = []
    for index, row in city_expense.iterrows():
        expense_number, expense_year = row['effort'].split(" / ")
        expense_number.replace("-", "")
        detailed_expenses = scrapper.get_detailed_expense(expense_number, expense_year)
        detailed_expenses_list.append(detailed_expenses)

    detailed_expenses_df = pd.DataFrame.from_dict(detailed_expenses_list)

    object_name = f"detailed_expens/{city_name}-expense-{partition_date_str}"
    s3_resource.upload_object(bucket, object_name, detailed_expenses_df)

    # query_cursor = postgres_resource.execute_query(
    #     SELECT_LAST_REVENUE_DATE_FROM_MONTH.format(partition_date_str[:7])
    # )

    # last_revenue = False
    # try:
    #     last_revenue = query_cursor.one()
    # except NoResultFound:
    #     context.log.info("No record of previous revenue found in database")
    # if last_revenue:
    #     last_revenue_date = last_revenue[0]
    #     city_revenue.query(f'date > "{last_revenue_date}"', inplace=True)


    # postgres_resource.save_dataframe('expense', detailed_expenses_df)

    return Output(
        detailed_expenses_df,
        metadata={
            "Count": len(detailed_expenses_df),
            "preview": MetadataValue.md(detailed_expenses_df.head().to_markdown()),
        }
    )
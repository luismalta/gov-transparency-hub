import pandas as pd
from datetime import datetime
from io import StringIO, BytesIO
from sqlalchemy.exc import NoResultFound
from dagster import Output, asset, MetadataValue

from minas_data_lab.resources import PostgresResource, S3Resource
from minas_data_lab.resources.PortalTransparenciaScrapper import PortalTransparenciaScrapper

SELECT_LAST_REVENUE_DATE = 'select date from revenue order by date desc limit 1'


@asset
def city_revenue_file(context, s3_resource: S3Resource):
    """
        Raw report dowloaded from Portal da Transparencia
    """
    city_name = 'saojoaodelrei'
    month = datetime.now().month - 1
    payload = ['INT_EXR=2023', 'CHAR_ID_EMP=1', 'LG_ALT_PAG=S', f'INT_MES_INI={month}', f'INT_MES_FIM={month}', 'URL=Tempo_Real_Receitas']
    scrapper = PortalTransparenciaScrapper(city_name)
    report = scrapper.get_report('Tempo_Real_Receitas', 'csv', payload)

    city_revenue_df = pd.read_csv(StringIO(report), index_col=False)

    bucket = city_name
    object_name = f"{city_name}-revenue-{datetime.now().strftime('%d-%m-%Y-')}"
    s3_resource.upload_object(bucket, object_name, city_revenue_df)

@asset(
    deps=["city_revenue_file"],
)
def city_revenue(context, s3_resource: S3Resource, postgres_resource: PostgresResource):
    """
        Raw city revenue dataset, loaded into Postgres database
    """
    city_name = 'saojoaodelrei'
    bucket = city_name
    object_name = f"{city_name}-revenue-{datetime.now().strftime('%d-%m-%Y-')}"
    city_revenue = s3_resource.get_object(bucket, object_name)

    columns_rename = {
        'Data': 'date',
        'Tipo de Minuta': 'draft_type',
        'Receita': 'revenue',
        'Unnamed: 3': 'source',
        'Fonte de Recursos': 'resource_source',
        'CO TCE': 'co_tce',
        'CO AUX': 'co_aux',
        'Histórico': 'historic',
        'Valor': 'value',
    }
    city_revenue.query('not Data.str.contains("TOTAL")', engine='python', inplace=True)
    city_revenue.query('not Data.str.contains("Total do Dia")', engine='python', inplace=True)
    city_revenue.rename(columns=columns_rename, inplace=True)

    city_revenue['date'] = pd.to_datetime(city_revenue['date'], format="%d/%m/%Y")

    query_cursor = postgres_resource.execute_query(SELECT_LAST_REVENUE_DATE)

    last_revenue = False
    try:
        last_revenue = query_cursor.one()
    except NoResultFound:
        context.log.info("No record of previous revenue found in database")
    if last_revenue:
        last_revenue_date = last_revenue[0]
        city_revenue.query(f'date > "{last_revenue_date}"', inplace=True)

    postgres_resource.save_dataframe('revenue', city_revenue)

    return Output(
        city_revenue,
        metadata={
            "Count": len(city_revenue),
            "preview": MetadataValue.md(city_revenue.head().to_markdown()),
        }
    )
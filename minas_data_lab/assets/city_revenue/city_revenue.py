import pandas as pd
from io import StringIO
from datetime import datetime
from dagster import Output, asset, MetadataValue

from minas_data_lab.resources.PortalTransparenciaScrapper import PortalTransparenciaScrapper

SELECT_LAST_REVENUE_DATE = 'select date from revenue order by date desc limit 1'


@asset(
    io_manager_key="s3_io_manager",
    key_prefix=["revenue"],
)
def city_revenue(context) -> pd.DataFrame:
    month = datetime.now().month - 1
    payload = ['INT_EXR=2023', 'CHAR_ID_EMP=1', 'LG_ALT_PAG=S', f'INT_MES_INI={month}', f'INT_MES_FIM={month}', 'URL=Tempo_Real_Receitas']
    scrapper = PortalTransparenciaScrapper('saojoaodelrei')
    report = scrapper.get_report('Tempo_Real_Receitas', 'csv', payload)

    city_revenue = pd.read_csv(StringIO(report), index_col=False)
    return city_revenue


@asset(
    io_manager_key="postgres_io_manager",
    required_resource_keys={"postgres_resource"},
    key_prefix=["revenue"],
)
def city_revenue_cleaned(context, city_revenue: pd.DataFrame) -> Output[pd.DataFrame]:
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

    query_cursor = context.resources.postgres_resource.execute_query(SELECT_LAST_REVENUE_DATE)
    last_revenue_date = query_cursor.one()[0]
    city_revenue.query(f'date > "{last_revenue_date}"', inplace=True)

    return Output(
        city_revenue,
        metadata={
            "Count": len(city_revenue),
            "preview": MetadataValue.md(city_revenue.head().to_markdown()),
        }
    )
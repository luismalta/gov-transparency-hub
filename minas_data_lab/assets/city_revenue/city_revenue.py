import pandas as pd
from io import StringIO
from dagster import Output, asset, MetadataValue

from minas_data_lab.resources.PortalTransparenciaScrapper import PortalTransparenciaScrapper


@asset(
    io_manager_key="s3_io_manager",
    key_prefix=["revenue"],
)
def city_revenue(context) -> pd.DataFrame:
    payload = ['INT_EXR=2023', 'CHAR_ID_EMP=1', 'LG_ALT_PAG=S', 'URL=Tempo_Real_Receitas']
    scrapper = PortalTransparenciaScrapper('saojoaodelrei')
    report = scrapper.get_report('Tempo_Real_Receitas', 'csv', payload)

    city_revenue = pd.read_csv(StringIO(report), index_col=False)
    return city_revenue


@asset(
    io_manager_key="postgres_io_manager",
    key_prefix=["revenue"],
)
def city_revenue_cleaned(city_revenue: pd.DataFrame) -> Output[pd.DataFrame]:
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

    return Output(
        city_revenue,
        metadata={
            "Count": len(city_revenue),
            "preview": MetadataValue.md(city_revenue.head().to_markdown()),
        }
    )
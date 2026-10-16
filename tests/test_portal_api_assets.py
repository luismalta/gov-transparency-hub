import pytest
from dagster import AssetKey

from gov_transparency_hub import defs
from gov_transparency_hub.assets.df_reports import DF_REPORT_ENDPOINTS
from gov_transparency_hub.assets.df_resources import DF_RESOURCE_ENDPOINTS
from gov_transparency_hub.assets.portal_api_assets import (
    _partition_params,
    volume_object_path,
)
from gov_transparency_hub.partitions import (
    city_partition,
    daily_city_partition,
    monthly_city_partition,
    partitions_def_for,
    yearly_city_partition,
)

YEARLY = [
    "receita", "livro_de_lancamentos", "orcamento_anual_de_despesa",
    "orcamento_anual_de_receita", "co_tce", "unidades_orcamentarias",
    "fontes_de_recursos", "receitas", "co_aux", "funcoes", "subfuncoes",
    "programas", "projetos", "categorias_economicas", "tipos_de_despesas",
]
CITY_ONLY = [
    "tipos_de_empenhos", "unidades_gestoras", "tipos_de_licitacao",
    "situacoes_da_licitacao", "processos_da_licitacao",
]
EXPECTED = {
    "despesa": daily_city_partition,
    "licitacoes": monthly_city_partition,
    **dict.fromkeys(YEARLY, yearly_city_partition),
    **dict.fromkeys(CITY_ONLY, city_partition),
}


def test_every_endpoint_has_its_natural_granularity():
    actual = {
        e.endpoint: partitions_def_for(e.param_names)
        for e in DF_REPORT_ENDPOINTS + DF_RESOURCE_ENDPOINTS
    }

    assert actual == EXPECTED


@pytest.mark.parametrize(
    ("param_names", "period", "expected"),
    [
        (("exercicio", "data_do_empenho_inicial", "data_do_empenho_final"), "2025-01-02",
         {"exercicio": "2025", "data_do_empenho_inicial": "2025-01-02", "data_do_empenho_final": "2025-01-02"}),
        (("exercicio_de_criacao", "mes_de_criacao"), "2025-03",
         {"exercicio_de_criacao": "2025", "mes_de_criacao": "03"}),
        (("exercicio", "unidade_gestora"), "2025", {"exercicio": "2025"}),
        ((), None, {}),
    ],
)
def test_params_come_from_the_period(param_names, period, expected):
    assert _partition_params(param_names, period) == expected


def test_volume_path_includes_period_only_when_there_is_one():
    assert volume_object_path("receita", "tiradentes", "2025") == (
        "tiradentes/receita/tiradentes-receita-2025.parquet"
    )
    assert volume_object_path("unidades_gestoras", "tiradentes", None) == (
        "tiradentes/unidades_gestoras/tiradentes-unidades_gestoras.parquet"
    )


def test_assets_use_their_endpoint_partitions():
    graph = defs.resolve_asset_graph()

    assert graph.get(AssetKey("df_receita")).partitions_def == yearly_city_partition
    assert graph.get(AssetKey("df_unidades_gestoras")).partitions_def == city_partition


def test_per_ug_assets_still_depend_on_unidades_gestoras():
    graph = defs.resolve_asset_graph()

    for name in ("df_despesa", "df_receita", "df_licitacoes"):
        assert AssetKey("df_unidades_gestoras") in graph.get(AssetKey(name)).parent_keys

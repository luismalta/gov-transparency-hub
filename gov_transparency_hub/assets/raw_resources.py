from gov_transparency_hub.assets.raw_load_assets import (
    RawTableConfig,
    build_raw_load_assets,
)

CITY_KEY = ("municipio", "id")
UG_YEAR_KEY = ("municipio", "unidade_gestora", "ano_referencia")

RAW_RESOURCE_CONFIGS = [
    RawTableConfig("tipos_de_empenhos", CITY_KEY),
    RawTableConfig("unidades_gestoras", CITY_KEY),
    RawTableConfig("tipos_de_licitacao", CITY_KEY),
    RawTableConfig("situacoes_da_licitacao", CITY_KEY),
    RawTableConfig("processos_da_licitacao", CITY_KEY),
    RawTableConfig("co_tce", ("municipio", "ano_referencia", "id")),
    RawTableConfig("unidades_orcamentarias", (*UG_YEAR_KEY, "id")),
    # The API repeats some rows verbatim; they carry no extra information.
    RawTableConfig(
        "fontes_de_recursos", (*UG_YEAR_KEY, "id"), drop_exact_duplicates=True
    ),
    RawTableConfig("receitas", (*UG_YEAR_KEY, "codigo_da_receita")),
    RawTableConfig("co_aux", (*UG_YEAR_KEY, "id")),
    RawTableConfig("funcoes", (*UG_YEAR_KEY, "id")),
    RawTableConfig("subfuncoes", (*UG_YEAR_KEY, "id")),
    RawTableConfig("programas", (*UG_YEAR_KEY, "id")),
    RawTableConfig("projetos", (*UG_YEAR_KEY, "id")),
    RawTableConfig("categorias_economicas", (*UG_YEAR_KEY, "id")),
    # id is only the 2-digit sub-element suffix (hierarchy lost by the API).
    RawTableConfig("tipos_de_despesas", UG_YEAR_KEY, strategy="delete-insert"),
]

raw_resource_assets = build_raw_load_assets(
    "raw_resources", RAW_RESOURCE_CONFIGS
)

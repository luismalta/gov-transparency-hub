from gov_transparency_hub.assets.df_reports import DF_REPORT_ENDPOINTS
from gov_transparency_hub.assets.raw_load_assets import (
    RawTableConfig,
    build_raw_load_assets,
)

# Keys validated against the 2024/2025 portal downloads; see
# docs/superpowers/specs/2026-10-12-raw-load-databricks-design.md.
RAW_REPORT_CONFIGS = [
    RawTableConfig(
        "despesa",
        ("municipio", "unidade_gestora", "ano_referencia", "empenho", "tipo"),
        flatten_column="dadosPrincipais",
    ),
    # No natural key: id is the revenue nature code, repeated every day.
    RawTableConfig(
        "receita",
        ("municipio", "unidade_gestora", "ano_referencia"),
        strategy="delete-insert",
    ),
    RawTableConfig(
        "livro_de_lancamentos",
        ("municipio", "unidade_gestora", "data", "numero"),
    ),
    RawTableConfig(
        "licitacoes", ("municipio", "unidade_gestora", "numero_do_processo")
    ),
    RawTableConfig(
        "orcamento_anual_de_despesa", ("municipio", "ano_referencia", "ficha")
    ),
    RawTableConfig(
        "orcamento_anual_de_receita",
        ("municipio", "ano_referencia", "codigo_da_receita"),
    ),
]

raw_report_assets = build_raw_load_assets(
    "raw_reports", RAW_REPORT_CONFIGS, DF_REPORT_ENDPOINTS
)

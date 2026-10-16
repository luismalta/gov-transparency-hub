from gov_transparency_hub.assets.portal_api_assets import (
    PortalEndpoint,
    build_portal_api_assets,
)

# Timeouts are sized from observed response times (Tiradentes, 2025):
# orcamento_anual_de_despesa ~30s, livro_de_lancamentos ~71s, receita ~116s,
# despesa (one day) ~22s, licitacoes (one month) up to ~29s.
DF_REPORT_ENDPOINTS = [
    PortalEndpoint("orcamento_anual_de_receita", "orcamentoAnualDeReceita", ("exercicio",)),
    PortalEndpoint(
        "orcamento_anual_de_despesa", "orcamentoAnualDeDespesa", ("exercicio",), 120
    ),
    PortalEndpoint("receita", "receitas", ("exercicio", "unidade_gestora"), 300),
    PortalEndpoint(
        "livro_de_lancamentos",
        "livroDeLancamentos",
        ("exercicio", "unidade_gestora"),
        180,
    ),
    PortalEndpoint(
        "despesa",
        "despesas",
        (
            "exercicio",
            "unidade_gestora",
            "data_do_empenho_inicial",
            "data_do_empenho_final",
        ),
        120,
    ),
    PortalEndpoint(
        "licitacoes",
        "licitacoes",
        ("unidade_gestora", "exercicio_de_criacao", "mes_de_criacao"),
        120,
    ),
]

df_report_assets = build_portal_api_assets(
    "relatorios", "df_reports", DF_REPORT_ENDPOINTS
)

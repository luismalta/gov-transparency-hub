from gov_transparency_hub.assets.portal_api_assets import (
    PortalEndpoint,
    build_portal_api_assets,
)

UG_PARAMS = ("exercicio", "unidade_gestora")

DF_RESOURCE_ENDPOINTS = [
    PortalEndpoint("tipos_de_empenhos", "tiposDeEmpenhos"),
    PortalEndpoint("unidades_gestoras", "unidadesGestoras"),
    PortalEndpoint("tipos_de_licitacao", "tiposDeLicitacao"),
    PortalEndpoint("situacoes_da_licitacao", "situacoesDaLicitacao"),
    PortalEndpoint("processos_da_licitacao", "processosDaLicitacao"),
    PortalEndpoint("co_tce", "codigosTce", ("exercicio",)),
    PortalEndpoint("unidades_orcamentarias", "unidadesOrcamentarias", UG_PARAMS),
    PortalEndpoint("fontes_de_recursos", "fontesDeRecursos", UG_PARAMS),
    PortalEndpoint("receitas", "receitas", UG_PARAMS),
    PortalEndpoint("co_aux", "codigosAux", UG_PARAMS),
    PortalEndpoint("funcoes", "funcoes", UG_PARAMS),
    PortalEndpoint("subfuncoes", "subfuncoes", UG_PARAMS),
    PortalEndpoint("programas", "programas", UG_PARAMS),
    PortalEndpoint("projetos", "projetos", UG_PARAMS),
    PortalEndpoint("categorias_economicas", "categoriasEconomicas", UG_PARAMS),
    PortalEndpoint("tipos_de_despesas", "tiposDeDespesas", UG_PARAMS),
]

df_resource_assets = build_portal_api_assets(
    "dados_complementares", "df_resources", DF_RESOURCE_ENDPOINTS
)

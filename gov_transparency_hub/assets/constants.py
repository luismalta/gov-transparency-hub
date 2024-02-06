START_DATE = "2023-01-01"
CITY_LIST = [
    'tiradentes',
    'saojoaodelrei',
    'saotiago',
    'piedadedoriogrande',
    'cbm',
    'itutinga',
    'carrancas',

    'santacruzdeminas',
    'ritapolis',
    'prados',
    'resendecosta',
    'nazareno',
    'madrededeusdeminas',
    'lagoadourada',
    'doresdecampos',
    'coronelxavierchaves',
    'itumirim',
    'ingai',
    'santabarbaradotugurio',
    # 'ressaquinha',  # Blacklisted
    # 'ibertioga',  # Blacklisted
    'desterrodeentrerios',
    'carandai',
    'caranaiba',
    'barroso',
    'municipioantoniocarlos',
    'alfredovasconcelos',

    'santanadogarambeu',
    'luminarias',

    'ijaci',
    # 'capelanova'  # Blacklisted
]

CITY_EXPENSE_COLUMNS_RENAME = {
    "Número": "number",
    "Exercício": "year",
    "Tipo": "type",
    "Data do Empenho": "effort_date",
    "Liquidação": "settlement_date",
    "Pagamento": "payment_date",
    "Unidade": "unit",
    "Função": "function",
    "Subfunção": "subfunction",
    "Programa": "program",
    "Projeto Atividade": "activity",
    "Categoria Econômica": "economic_category",
    "Fonte de Recurso": "resource_source",
    "CO TCE": "co_tce",
    "CO AUX": "co_aux",
    "Valor": "value",
    "Beneficiário": "creditor",
    "CPF/CNPJ": "cpf_cnpj",
    "Histórico": "history",
}

CITY_REVENUE_COLUMNS_RENAME = {
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
with despesa as (

    select * from {{ ref('stg_despesa') }}

)

select
    -- tipo_descricao is part of the grain: the cadastro has several tipos with
    -- code OP (ordem de pagamento, restos a pagar, transferências) and raw keys
    -- on the full tipo text.
    {{ dbt_utils.generate_surrogate_key(['d.municipio', 'd.unidade_gestora', 'd.ano_referencia', 'd.empenho', 'd.tipo_codigo', 'd.tipo_descricao']) }} as despesa_sk,
    mun.municipio_sk,
    ug.unidade_gestora_sk,
    uo.unidade_orcamentaria_sk,
    fn.funcao_sk,
    sf.subfuncao_sk,
    pg.programa_sk,
    ac.acao_sk,
    ce.categoria_economica_sk,
    fr.fonte_recurso_sk,
    cr.credor_sk,
    de.data_sk as data_empenho_sk,
    dl.data_sk as data_liquidacao_sk,
    dp.data_sk as data_pagamento_sk,
    d.municipio,
    d.unidade_gestora,
    d.ano_referencia,
    d.empenho,
    d.tipo_codigo,
    d.tipo_descricao,
    d.natureza,
    d.exercicio,
    d.historico,
    d.valor
from despesa as d
left join {{ ref('dim_municipio') }} as mun
    on mun.municipio = d.municipio
left join {{ ref('dim_unidade_gestora') }} as ug
    on ug.municipio = d.municipio
    and ug.unidade_gestora = d.unidade_gestora
-- Classification joins use the partition year, not exercicio: restos a pagar
-- (OP from older exercícios) are classified with the cadastro of the year
-- they were paid in.
{%- for alias, dim, code in [
    ('uo', 'dim_unidade_orcamentaria', 'unidade_orcamentaria_codigo'),
    ('fn', 'dim_funcao', 'funcao_codigo'),
    ('sf', 'dim_subfuncao', 'subfuncao_codigo'),
    ('pg', 'dim_programa', 'programa_codigo'),
    ('ac', 'dim_acao', 'acao_codigo'),
    ('ce', 'dim_categoria_economica', 'categoria_economica_codigo'),
    ('fr', 'dim_fonte_recurso', 'fonte_recurso_codigo'),
] %}
left join {{ ref(dim) }} as {{ alias }}
    on {{ alias }}.municipio = d.municipio
    and {{ alias }}.unidade_gestora = d.unidade_gestora
    and {{ alias }}.ano_referencia = d.ano_referencia
    and {{ alias }}.codigo = d.{{ code }}
{%- endfor %}
left join {{ ref('dim_credor') }} as cr
    on cr.credor_documento <=> d.credor_documento
    and cr.credor_nome <=> d.credor_nome
left join {{ ref('dim_data') }} as de
    on de.data = d.data_empenho
left join {{ ref('dim_data') }} as dl
    on dl.data = d.data_liquidacao
left join {{ ref('dim_data') }} as dp
    on dp.data = d.data_pagamento

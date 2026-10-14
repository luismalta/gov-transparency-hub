with orcamento as (

    select * from {{ ref('stg_orcamento_anual_de_despesa') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['o.municipio', 'o.ano_referencia', 'o.ficha']) }} as orcamento_despesa_sk,
    mun.municipio_sk,
    uo.unidade_orcamentaria_sk,
    fn.funcao_sk,
    sf.subfuncao_sk,
    pg.programa_sk,
    ac.acao_sk,
    ce.categoria_economica_sk,
    o.municipio,
    o.ano_referencia,
    o.ficha,
    o.valor_orcado,
    o.valor_suplementado_ou_anulado,
    o.valor_atualizado,
    o.saldo_atual
from orcamento as o
left join {{ ref('dim_municipio') }} as mun
    on mun.municipio = o.municipio
{%- for alias, dim, code in [
    ('uo', 'dim_unidade_orcamentaria', 'unidade_orcamentaria_codigo'),
    ('fn', 'dim_funcao', 'funcao_codigo'),
    ('sf', 'dim_subfuncao', 'subfuncao_codigo'),
    ('pg', 'dim_programa', 'programa_codigo'),
    ('ac', 'dim_acao', 'acao_codigo'),
    ('ce', 'dim_categoria_economica', 'categoria_economica_codigo'),
] %}
left join {{ ref(dim) }} as {{ alias }}
    on {{ alias }}.municipio = o.municipio
    and {{ alias }}.ano_referencia = o.ano_referencia
    and {{ alias }}.codigo = o.{{ code }}
{%- endfor %}

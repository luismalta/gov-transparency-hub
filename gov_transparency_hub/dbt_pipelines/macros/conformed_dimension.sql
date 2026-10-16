{#- Classification dimension conformed by (municipio, ano_referencia, codigo):
    the union of every UG's cadastro, plus members parsed from fact text for
    codes no cadastro has (e.g. the Câmara when it is not a portal UG).
    Name: any non-null name beats a null one (a fact name fills a nameless
    cadastro code, and origem then says 'fato'); then cadastro beats fact;
    most frequent wins; ties -> smallest name. -#}
{% macro conformed_dimension(staging_cadastro, key_name, fact_members) -%}
with cadastro as (

    select municipio, ano_referencia, codigo, nome, count(*) as frequencia
    from {{ ref(staging_cadastro) }}
    where codigo is not null
    group by municipio, ano_referencia, codigo, nome

),

fato as (

    select municipio, ano_referencia, codigo, nome, count(*) as frequencia
    from (
        {%- for model, code_column, name_column in fact_members %}
        select municipio, ano_referencia, {{ code_column }} as codigo, {{ name_column }} as nome
        from {{ ref(model) }}
        where {{ code_column }} is not null
        {% if not loop.last %}union all{% endif %}
        {%- endfor %}
    )
    group by municipio, ano_referencia, codigo, nome

),

candidatos as (

    select *, 'cadastro' as origem, 1 as prioridade from cadastro
    union all
    select *, 'fato' as origem, 2 as prioridade from fato

),

escolhido as (

    select
        *,
        row_number() over (
            partition by municipio, ano_referencia, codigo
            order by nome is null, prioridade, frequencia desc, nome asc
        ) as posicao
    from candidatos

)

select
    {{ dbt_utils.generate_surrogate_key(['municipio', 'ano_referencia', 'codigo']) }} as {{ key_name }},
    municipio,
    ano_referencia,
    codigo,
    nome,
    origem
from escolhido
where posicao = 1
{%- endmacro %}

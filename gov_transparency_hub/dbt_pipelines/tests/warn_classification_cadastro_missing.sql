{{ config(severity='warn') }}

{#- Fallback members keep FKs resolvable, which would also hide a cadastro
    that was never loaded. A (municipio, ano) whose dimension has no
    cadastro member at all points to missing raw_<cadastro> data. -#}
{% set dims = [
    'dim_unidade_orcamentaria', 'dim_funcao', 'dim_subfuncao', 'dim_programa',
    'dim_acao', 'dim_categoria_economica', 'dim_fonte_recurso',
] %}

{% for dim in dims %}
select '{{ dim }}' as dimensao, municipio, ano_referencia, count(*) as membros
from {{ ref(dim) }}
group by municipio, ano_referencia
having count_if(origem = 'cadastro') = 0
{% if not loop.last %}union all{% endif %}
{% endfor %}

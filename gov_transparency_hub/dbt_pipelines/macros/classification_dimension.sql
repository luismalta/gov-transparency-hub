{#- Classification cadastros become one dimension each, keyed like raw:
    (municipio, unidade_gestora, ano_referencia, codigo). -#}
{% macro classification_dimension(staging_model, key_name) -%}
select
    {{ dbt_utils.generate_surrogate_key(['municipio', 'unidade_gestora', 'ano_referencia', 'codigo']) }} as {{ key_name }},
    municipio,
    unidade_gestora,
    ano_referencia,
    codigo,
    nome
from {{ ref(staging_model) }}
{%- endmacro %}

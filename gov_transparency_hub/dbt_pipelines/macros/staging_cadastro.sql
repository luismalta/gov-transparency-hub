{#- Cadastros (funcoes, programas, ...) share one shape per UG and year. -#}
{% macro staging_cadastro(table_name) -%}
select
    municipio,
    unidade_gestora,
    ano_referencia,
    {{ normalize_code('id') }} as codigo,
    nullif(trim(nome), '') as nome
from {{ source('raw', table_name) }}
{%- endmacro %}

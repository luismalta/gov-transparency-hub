{#- Portal text fields come as "<codigo> - <nome>"; codes may carry dots
    ("3.3.90.39.00") that the cadastro ids do not have ("33903900"). -#}

{% macro normalize_code(column) -%}
    nullif(replace(trim({{ column }}), '.', ''), '')
{%- endmacro %}

{% macro split_code(column) -%}
    {{ normalize_code("split_part(" ~ column ~ ", ' - ', 1)") }}
{%- endmacro %}

{% macro split_name(column) -%}
    case
        when instr({{ column }}, ' - ') > 0
            then nullif(trim(substring({{ column }}, instr({{ column }}, ' - ') + 3)), '')
    end
{%- endmacro %}

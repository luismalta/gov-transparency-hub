{{ config(materialized='table') }}

with raw_data as (
    select *
    from {{ source('dagster', 'expense_itens') }}
    where expense_number != 'N/A'
),

cleaned_data as (
    select
        "surrogate_key"::varchar as surrogate_key,
        "item",
        "expense_number" as numero_despesa,
        "expense_year" as ano_despesa,
        "complemento",
        "unidade",
        "marca",
        case 
            when REPLACE(REPLACE("quantidade", '.', ''), ',', '.') ~ '^[0-9]+(\.[0-9]+)?$'
            then REPLACE(REPLACE("quantidade", '.', ''), ',', '.')::DECIMAL
            else null
        end as quantidade,
        case 
            when REPLACE(REPLACE("valor_unita_rio", '.', ''), ',', '.') ~ '^[0-9]+(\.[0-9]+)?$'
            then REPLACE(REPLACE("valor_unita_rio", '.', ''), ',', '.')::DECIMAL
            else null
        end as valor_unitario,
        case 
            when REPLACE(REPLACE("total", '.', ''), ',', '.') ~ '^[0-9]+(\.[0-9]+)?$'
            then REPLACE(REPLACE("total", '.', ''), ',', '.')::DECIMAL
            else null
        end as total,
        "municipio"
    from raw_data
),

valid_records as (
    select *
    from cleaned_data
    where quantidade is not null
      and valor_unitario is not null
      and total is not null
)

select *
from valid_records

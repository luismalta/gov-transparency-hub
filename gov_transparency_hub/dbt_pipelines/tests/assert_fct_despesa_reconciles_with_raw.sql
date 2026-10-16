with fact as (

    select count(*) as fact_rows, sum(valor) as fact_valor
    from {{ ref('fct_despesa') }}

),

raw_despesa as (

    select count(*) as raw_rows, sum(cast(valor as decimal(18, 2))) as raw_valor
    from {{ source('raw', 'despesa') }}

)

select *
from fact
cross join raw_despesa
where fact_rows <> raw_rows or fact_valor <> raw_valor

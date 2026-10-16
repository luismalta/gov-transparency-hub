with fact as (

    select count(*) as fact_rows, sum(valor_orcado) as fact_valor
    from {{ ref('fct_orcamento_despesa') }}

),

raw_orcamento as (

    select count(*) as raw_rows, sum(cast(valor_orcado as decimal(18, 2))) as raw_valor
    from {{ source('raw', 'orcamento_anual_de_despesa') }}

)

select *
from fact
cross join raw_orcamento
where fact_rows <> raw_rows or fact_valor <> raw_valor

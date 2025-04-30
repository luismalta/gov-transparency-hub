-- Active: 1718368045625@@127.0.0.1@5432@gov_transparency_hub
{{ config(materialized='table') }}


with revenue_details as (

    SELECT
        "id"::varchar,
        TO_DATE("data", 'DD/MM/YYY') AS "data",
        "tipo_de_minuta" AS "tipo_minuta",
        "receita" AS "codigo_receita",
        "unnamed_3" AS "descricao_receita",
        "fonte_de_recursos" AS "fonte_recurso",
        "co_tce",
        "co_aux",
        "hist_rico" AS "historico",
        "valor",
        "municipio"
    FROM
    {{ source('dagster', 'revenue_details') }}

)

select *
from revenue_details

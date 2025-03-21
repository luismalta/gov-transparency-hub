-- Active: 1718368045625@@127.0.0.1@5432@gov_transparency_hub
{{ config(materialized='table') }}


with expense_itens as (

    SELECT
        "item",
        "expense_number" AS "numero_despesa",
        "expense_year" AS "ano_despesa",
        "complemento" AS "complemento",
        "unidade" AS "unidade",
        "marca" AS "marca",
        REPLACE(REPLACE("quantidade", '.', ''), ',', '.')::DECIMAL AS "quantidade",
        REPLACE(REPLACE("valor_unita_rio", '.', ''), ',', '.')::DECIMAL AS "valor_unitario",
        REPLACE(REPLACE("total", '.', ''), ',', '.')::DECIMAL AS "total",
        "municipio" AS "municipio"
    FROM
    {{ source('dagster', 'expense_itens') }}

)

select *
from expense_itens

-- Active: 1718368045625@@127.0.0.1@5432@gov_transparency_hub
{{ config(materialized='table') }}

with delete_na_rows as (
    SELECT *
    FROM {{ source('dagster', 'expense_invoices') }}
    WHERE
        expense_number != 'N/A'
),

expense_invoices as (

    SELECT
        "surrogate_key"::varchar,
        "codigo",
        "tipo",
        "expense_number" AS "numero_despesa",
        "expense_year" AS "ano_despesa",
        "nota_fiscal" AS "nota_fiscal",
        "se_rie" AS "serie",
        TO_DATE("emissa_o", 'DD/MM/YYY') AS "data_emissao",
        TO_DATE("vencimento", 'DD/MM/YYY') AS "data_vencimento",
        "chave_de_acesso" AS "chave_acesso",
        "municipio" AS "municipio"
    FROM
        delete_na_rows

)

select *
from expense_invoices

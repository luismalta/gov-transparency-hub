-- Active: 1718368045625@@127.0.0.1@5432@gov_transparency_hub
{{ config(materialized='table') }}

with delete_na_rows as (
    SELECT *
    FROM {{ source('dagster', 'expense_details') }}
    WHERE
        numero != 'N/A'
),

expense_details as (

    SELECT
        "numero",
        "ano" AS "ano",
        "tipo" AS "tipo",
        TO_DATE("data_do_empenho", 'DD/MM/YYYY') AS "data_empenho",
        TO_DATE("liquidac_a_o", 'DD/MM/YYYY') AS "data_liquidacao",
        TO_DATE("pagamento", 'DD/MM/YYYY') AS "data_pagamento",
        "unidade" AS "unidade",
        "func_a_o" AS "funcao",
        "subfunc_a_o" AS "subfuncao",
        "programa" AS "programa",
        "projeto_atividade" AS "atividade",
        "categoria_econo_mica" AS "categoria_economica",
        "fonte_de_recurso" AS "fonte_recurso",
        "co_tce" AS "co_tce",
        "co_aux" AS "co_aux",
        REPLACE(REPLACE("valor", '.', ''), ',', '.')::DECIMAL AS "valor",
        "beneficia_rio" AS "beneficiario",
        "cpf_cnpj" AS "cpf_cnpj",
        "histo_rico" AS "historico",
        "municipio" AS "municipio"
    FROM
        delete_na_rows

)

select *
from expense_details

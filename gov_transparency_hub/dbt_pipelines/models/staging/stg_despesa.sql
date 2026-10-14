with source as (

    select * from {{ source('raw', 'despesa') }}

),

credor as (

    select
        *,
        nullif(regexp_extract(credor, r'CPF/CNPJ:\s*(\S+)\s*$', 1), '') as credor_documento_raw,
        nullif(trim(regexp_replace(credor, r'\s*-\s*CPF/CNPJ:.*$', '')), '') as credor_nome_raw
    from source

)

select
    municipio,
    unidade_gestora,
    ano_referencia,
    empenho,
    {{ split_code('tipo') }} as tipo_codigo,
    {{ split_name('tipo') }} as tipo_descricao,
    -- Explicit on purpose: the cadastro also lists annulments, blocks and
    -- cancellations (AN, AS, BL, AB, CP, ...); those must not be summed as
    -- commitments, so anything unmapped stays null.
    case
        when {{ split_code('tipo') }} in ('EO', 'EE', 'EG') then 'empenho'
        when {{ split_code('tipo') }} = 'OP' then 'ordem_pagamento'
    end as natureza,
    exercicio,
    cast(nullif(trim(data_do_empenho), '') as date) as data_empenho,
    cast(nullif(trim(data_de_liquidacao), '') as date) as data_liquidacao,
    cast(nullif(trim(data_de_pagamento), '') as date) as data_pagamento,
    {{ split_code('unidade') }} as unidade_orcamentaria_codigo,
    {{ split_code('funcao') }} as funcao_codigo,
    {{ split_code('subfuncao') }} as subfuncao_codigo,
    {{ split_code('programa') }} as programa_codigo,
    {{ split_code('projeto_atividade') }} as acao_codigo,
    {{ split_code('categoria_economica') }} as categoria_economica_codigo,
    {{ split_code('fonte_de_recurso') }} as fonte_recurso_codigo,
    credor_nome_raw as credor_nome,
    credor_documento_raw as credor_documento,
    case
        when credor_documento_raw like '%/%' then 'PJ'
        when credor_documento_raw is not null then 'PF'
    end as credor_tipo_pessoa,
    cast(valor as decimal(18, 2)) as valor,
    historico
from credor

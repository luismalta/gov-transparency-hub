select
    municipio,
    ano_referencia,
    trim(ficha) as ficha,
    {{ split_code('unidade_orcamentaria') }} as unidade_orcamentaria_codigo,
    {{ split_name('unidade_orcamentaria') }} as unidade_orcamentaria_nome,
    {{ split_code('funcao') }} as funcao_codigo,
    {{ split_name('funcao') }} as funcao_nome,
    {{ split_code('subfuncao') }} as subfuncao_codigo,
    {{ split_name('subfuncao') }} as subfuncao_nome,
    {{ split_code('programa') }} as programa_codigo,
    {{ split_name('programa') }} as programa_nome,
    {{ split_code('projeto') }} as acao_codigo,
    {{ split_name('projeto') }} as acao_nome,
    -- conta is the elemento de despesa, same code space as categorias_economicas.
    {{ split_code('conta') }} as categoria_economica_codigo,
    {{ split_name('conta') }} as categoria_economica_nome,
    cast(valor_orcado as decimal(18, 2)) as valor_orcado,
    cast(valor_suplementado_ou_anulado as decimal(18, 2)) as valor_suplementado_ou_anulado,
    cast(valor_atualizado as decimal(18, 2)) as valor_atualizado,
    cast(saldo_atual as decimal(18, 2)) as saldo_atual
from {{ source('raw', 'orcamento_anual_de_despesa') }}

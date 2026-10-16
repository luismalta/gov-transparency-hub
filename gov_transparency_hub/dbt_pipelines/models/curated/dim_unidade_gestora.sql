select
    {{ dbt_utils.generate_surrogate_key(['municipio', 'unidade_gestora']) }} as unidade_gestora_sk,
    municipio,
    unidade_gestora,
    nome
from {{ ref('stg_unidades_gestoras') }}

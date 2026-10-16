select distinct
    {{ dbt_utils.generate_surrogate_key(['credor_documento', 'credor_nome']) }} as credor_sk,
    credor_documento,
    credor_nome,
    credor_tipo_pessoa
from {{ ref('stg_despesa') }}
where credor_nome is not null or credor_documento is not null

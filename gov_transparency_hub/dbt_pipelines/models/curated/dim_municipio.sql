select
    {{ dbt_utils.generate_surrogate_key(['municipio']) }} as municipio_sk,
    municipio,
    nome
from {{ ref('municipios') }}

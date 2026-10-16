select
    municipio,
    trim(id) as unidade_gestora,
    nullif(trim(nome), '') as nome
from {{ source('raw', 'unidades_gestoras') }}

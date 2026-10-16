with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2015-01-01' as date)",
        end_date="cast('2031-01-01' as date)"
    ) }}

)

select
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as data_sk,
    cast(date_day as date) as data,
    year(date_day) as ano,
    month(date_day) as mes,
    day(date_day) as dia,
    quarter(date_day) as trimestre,
    dayofweek(date_day) as dia_semana
from spine

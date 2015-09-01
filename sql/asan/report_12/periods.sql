-- Параметры
with params as (
    select
        [[env.period.0]] as d_start,
        [[env.period.1]] as d_end,
        [[env.group_by]]::varchar as trunc_by,
        interval '1 {{env.group_by}}' as interv

        -- to_date('01.05.2015', 'dd.mm.yyyy') as d_start,
        -- to_date('15.05.2015', 'dd.mm.yyyy') as d_end,
        -- 'week'::varchar as trunc_by,
        -- interval '1 week' as interv

),

-- Генератор дат
dates as (
    select
        generate_series(date_trunc(trunc_by, d_start), date_trunc(trunc_by, d_end), interv)::date as d_start
    from params
)

select
    to_char(dates.d_start, 'yyyy-mm-dd') as period
from dates
cross join params

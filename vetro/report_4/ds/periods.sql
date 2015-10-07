with params as (
    select
        [[env.period.0]] as d_from,
        [[env.period.1]] as d_to

        -- to_date('01.05.2015', 'dd.mm.yyyy') as d_from,
        -- to_date('31.07.2015', 'dd.mm.yyyy') + interval '1 day - 1 second' as d_to
),

days as (
    select
        generate_series(params.d_from, params.d_to, interval '1 day')::date period
    from params
)


select
    to_char(days.period, 'yyyy_mm') as title,
    min(days.period) as d_start,
    max(days.period) + interval '1 day - 1 second' as d_end
from days
group by to_char(days.period, 'yyyy_mm')

union all

select
    to_char(days.period, 'yyyy') as title,
    min(days.period) as d_start,
    max(days.period) + interval '1 day - 1 second' as d_end
from days
group by to_char(days.period, 'yyyy')

order by d_end

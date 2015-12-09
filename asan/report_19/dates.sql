with base as (
    select generate_series([[env.period.0]], [[env.period.1]], '1 day')::date as d_start
)

select
    d_start,
    to_char(d_start, 'dd.mm.yyyy') as title
from base

with params as (
    select
    [[env.curator]]::integer as curator,
    [[env.inscompany]]::integer as inscompany,
    [[env.period.0]]::date as d_start,
    [[env.period.1]]::date as d_end

    -- 0 as curator,
    -- 0 as inscompany,
    -- current_date - 60 as d_start,
    -- current_date as d_end
),

periods as (
    select
        distinct date_trunc('month',  d.d_create) as dt
    from reports.v_document d
    cross join params
    where d.d_create between params.d_start and params.d_end
        and (params.inscompany = 0 or d.inscompany_id = params.inscompany)
        and (params.curator = 0 or d.curator_id = params.curator)
)

select * from (
    select
    	to_char(periods.dt, 'yyyy_mm') as title,
    	to_char(periods.dt, 'yyyy-mm-dd') as d_start,
    	to_char(periods.dt + interval '1 month', 'yyyy-mm-dd') as d_end
    from periods
    order by title
) t

union all

select
    'ИтогоВход' as title,
    '2015-01-01' as d_start,
    '2016-01-01' as d_end

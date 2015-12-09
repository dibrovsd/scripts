with params as (
    select
        {% if not env.period_type or env.period_type == 'month' %}
            date_trunc('month', current_date) as d_start,
            current_date + interval '1 day - 1 second' as d_end

        {% else %}
           [[env.period.0]] as d_start,
           [[env.period.1]] as d_end

        {% endif %}
           -- to_date('01.01.2014', 'dd.mm.yyyy') as d_start,
           -- current_date as d_end
),

calls as (
    select
        t.calldate at time zone 'Asia/Baku' as calldate,
        t.des as n_phone,
        t.duration
    from base_asteriskcall t
    cross join params
    where t.calldate between params.d_start and params.d_end
        and t.disposition = 'ANSWERED'
        and exists (
            select null from base_user
            where base_user.asterisk_ext::varchar = t.src
        )
    -- limit 100
),

calls_gr as (
    select
        date_trunc('day', calldate) as calldate,
        count(distinct n_phone) as cnt,
        --
        count(1) as cnt_all,
        sum(duration)::numeric / 60 as sum_duration
    from calls
    group by date_trunc('day', calldate)
),

sales_gr as (
    select
        date_trunc('day', t.d_issue) as d_issue,
        count(1) as cnt
    from reports.base_osago t
    where t.channel_root_id = 9
    group by date_trunc('day', t.d_issue)
),

res as (
    select
         calls_gr.calldate,
         calls_gr.cnt,
         calls_gr.sum_duration as duration,
         round(sales_gr.cnt::numeric / calls_gr.cnt * 100, 2) as sale_ratio
    from calls_gr
    left join sales_gr on sales_gr.d_issue = calls_gr.calldate

    union all

    select
         null as calldate,
         sum(calls_gr.cnt),
         sum(calls_gr.sum_duration) as duration,
         round(sum(sales_gr.cnt)::numeric / sum(calls_gr.cnt) * 100, 2) as sale_ratio
    from calls_gr
    left join sales_gr on sales_gr.d_issue = calls_gr.calldate

)

select
    res.calldate as "Дата звонка",
    res.cnt as "Кол-во звонков",
    round(res.duration, 2) as "Продолжительность (мин.)",
    res.sale_ratio as "Доля продаж",
    case
        when sale_ratio is null or res.calldate is null then ''
        when sale_ratio < 30 then 'background-color: #f2dede;'
        when sale_ratio < 50 then 'background-color: #F4A460;'
        when sale_ratio < 80 then 'background-color: #FFD700;'
        when sale_ratio < 100 then 'background-color: #ADFF2F;'
        else 'background-color: #98FB98;'
    end as sale_ratio_style
from res

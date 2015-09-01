with params as (
    select
        {% if not env.period_type or env.period_type == 'month' %}
            date_trunc('month', current_date) as d_start,
            current_date + interval '1 day - 1 second' as d_end

        {% else %}
           [[env.period.0]] as d_start,
           [[env.period.1]] as d_end

        {% endif %}

           -- to_date('01.01.2015', 'dd.mm.yyyy') as d_start,
           -- to_date('01.05.2015', 'dd.mm.yyyy') - 1 as d_end
),

calls as (
    select
        extract(hour from calldate) as callhour,
        count(1) as cnt,
        count(case when disposition = 'ANSWERED' then 1 end) as answered,
        count(case when disposition = 'BUSY' then 1 end) as busy,
        count(case when disposition = 'FAILED' then 1 end) as failed,
        count(case when disposition = 'NO ANSWER' then 1 end) as no_answer
    from base_asteriskcall t
    cross join params
    where t.calldate between params.d_start and params.d_end
    and exists (
        select null from base_user
        where base_user.asterisk_ext::varchar = t.src
    )
    group by extract(hour from calldate)
)

select
    callhour,
    -- Кол-во
    answered,
    busy,
    failed,
    no_answer,
    -- Процент
    round(answered::float / cnt * 100)||'%' as answered_ratio,
    round(busy::float / cnt * 100)||'%' as busy_ratio,
    round(failed::float / cnt * 100)||'%' as failed_ratio,
    round(no_answer::float / cnt * 100)||'%' as no_answer_ratio
from calls

union all

select
    null as callhour,
    -- Кол-во
    sum(answered),
    sum(busy),
    sum(failed),
    sum(no_answer),
    -- Процент
    null,
    null,
    null,
    null
from calls

order by callhour nulls last

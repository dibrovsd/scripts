-- Параметры
with params as (
    select
        {% if not env.period_type or env.period_type == 'month' %}
            date_trunc('month', current_date) as d_start,
            current_date + interval '1 day - 1 second' as d_end

        {% else %}
           [[env.period.0]] as d_start,
           [[env.period.1]] as d_end

        {% endif %}

        -- to_date('01.05.2015', 'dd.mm.yyyy') as d_start,
        -- current_date as d_end

),

gr as (

    -- ОСАГО
    select
        t.inscompany_id,
        sum(t.s_premium_base) as s_premium,
        count(1) as cnt
    from reports.base_osago t
    cross join params
    where t.d_issue between params.d_start and params.d_end
        {% if env.seller_territory == 'call_centre' %}
            and seller_territory_id = 9
        {% elif env.seller_territory == 'asan' %}
            and seller_territory_id != 9
        {% endif %}
    group by inscompany_id

)

select
    i.title as "Компания",
    s_premium / cnt as "Средняя премия"
from gr
join docflow_inscompany i on i.id = gr.inscompany_id

union all

select
    null,
    sum(s_premium) / sum(cnt)
from gr

order by "Компания"

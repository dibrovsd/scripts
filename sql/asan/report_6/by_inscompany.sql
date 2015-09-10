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

        -- to_date('01.01.2015', 'dd.mm.yyyy') as d_start,
        -- current_date as d_end

),

collect as (
    select
        t.inscompany_id,
        t.s_premium,
        t.s_comission,
        t.seller_territory_id
    from reports.base_sales t
    cross join params
    where t.d_issue between params.d_start and params.d_end
),

gr as (
    select
        inscompany_id,
        sum(s_premium) as s_premium,
        sum(s_comission) as s_comission
    from collect
    {% if env.seller_territory == 'call_centre' %}
        where seller_territory_id = 9
    {% elif env.seller_territory == 'asan' %}
        where seller_territory_id != 9
    {% endif %}
    group by inscompany_id
)

select
    i.title as inscompany,
    c.color,
    s_premium,
    s_comission
from gr
join docflow_inscompany i on i.id = gr.inscompany_id
left join (
    {{datasets.inscompany_color.sql}}
) c on c.id = gr.inscompany_id

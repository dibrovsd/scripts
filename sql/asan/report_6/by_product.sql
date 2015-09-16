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
        s.s_premium,
        s.seller_territory_id,
        s.product
    from reports.base_sales s
    cross join params
    where s.d_issue between params.d_start and params.d_end
)

select
    product,
    sum(s_premium) as s_premium
from collect
{% if env.seller_territory == 'call_centre' %}
    where seller_territory_id = 9
{% elif env.seller_territory == 'asan' %}
    where seller_territory_id != 9
{% endif %}
group by product

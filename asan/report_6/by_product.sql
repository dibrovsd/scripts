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

),

collect as (
    select
        s.s_premium,
        s.product
    from reports.base_sales s
    cross join params
    where s.d_issue between params.d_start and params.d_end

    {% if env.channel %}
        and s.channel_root_id = [[env.channel]]::integer
    {% endif %}

    {% if 'call_center' in user_params.territory_only %}
        and s.channel_root_id = 9
    {% elif 'asan' in user_params.territory_only %}
        and s.channel_root_id = 7
    {% endif %}
)

select
    product,
    sum(s_premium) as s_premium
from collect
group by product

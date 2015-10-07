with params as (
    select
        {% if get.period == 'день' %}
            current_date as d_start,
            80 as plan_sales

        {% else %}
            date_trunc('month', current_date) as d_start,
            2064 as plan_sales
        {% endif %}

        -- current_date as d_start,
        -- 80 as plan_sales
),

sales as (
    select
        count(1) as cnt,
        max(params.plan_sales) as plan_sales
    from reports.base_osago t
    cross join params
    where t.seller_territory_id = 9
        and t.d_issue > params.d_start
)

select
    sales.plan_sales,
    sales.cnt as fact_sales,
    round(sales.cnt::numeric / params.plan_sales * 100, 2) as plan_ratio,
    case
        when current_timestamp > (current_date + interval '17 hours') then 1
        else 0
    end as show_alert
from sales
cross join params

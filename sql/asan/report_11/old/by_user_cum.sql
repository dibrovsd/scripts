with params as (
    select
        {% if get.period == 'день' %}
            current_date as d_start
        {% else %}
            date_trunc('month', current_date) as d_start
        {% endif %}

        -- current_date as d_start
),

sales as (
    select s.seller_id, s.product
    from reports.base_sales s
    cross join params
    where s.seller_territory_id = 9
        and s.d_issue > params.d_start
)


select
    -- count(case when seller_id = 35 then 1 end) as "Вася",
    {% for u in datasets.users.data %}
        count(case when seller_id = {{u.id}} then 1 end) as "{{u.title}}",
    {% endfor %}
    0 as dummy
from sales

with sales as (
    select s.seller_id
    from reports.base_sales s
    where s.d_issue >= date_trunc('week', current_date)
)

select
    -- count(case when seller_id = 35 then 1 end) as "Вася",
    {% for u in datasets.users.data %}
        count(case when seller_id = {{u.id}} then 1 end) as "{{u.title}}",
    {% endfor %}
    0 as dummy,
    max(plan.plan_week) as plan_week,
    round(count(1)::numeric / max(plan.plan_week) * 100) as plan_ratio
from sales
cross join ({{datasets.settings.sql}}) plan

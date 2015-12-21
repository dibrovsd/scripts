with users as (
    select
        u.id,
        u.last_name ||' '|| u.first_name as user
    from base_user u
),

sales as (
    select
        s.seller_id,
        count(1) as cnt
    from reports.base_sales s
    where s.d_issue >= current_date
    group by s.seller_id
)

select
    u.user,
    coalesce(sales.cnt, 0) as cnt,
    plan.plan_day_user
from users u
left join sales on sales.seller_id = u.id
cross join ({{datasets.settings.sql}}) plan
order by sales.cnt desc nulls last
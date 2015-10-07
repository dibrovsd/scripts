with users as (
    select
        u.id,
        u.last_name ||' '|| u.first_name as user
    from base_user u
    where u.territory_id = 9
        and u.id != 51 -- fatima.huseynova
),

sales as (
    select
        s.seller_id,
        count(1) as cnt
    from reports.base_osago s
    where s.seller_territory_id = 9
      and s.d_issue >= current_date
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

with users as (
    select
        u.id,
        u.last_name ||' '|| u.first_name as user
    from base_user u
    inner join base_channel ch on ch.id = u.channel_id
    where ch.root_id = 9
        and u.id != 51 -- fatima.huseynova
        and u.id != 27 -- Shahsuvarova Lala
        and u.id != 29 -- Hasanova Sabina
        and u.id != 58 -- Stajer
        and u.id != 33 -- Babayeva Ayna
        and u.id != 28 -- Куратор КЦ
),

sales as (
    select
        s.seller_id,
        count(1) as cnt
    from reports.base_osago s
    where s.channel_root_id = 9
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
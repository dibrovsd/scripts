with sales as (
    select
        s.seller_id,
        count(1) as cnt
    from reports.base_sales s
    where s.channel_root_id = 9
        and s.seller_id != 51 -- fatima.huseynova
        and s.seller_id != 27 -- Shahsuvarova Lala
        and s.seller_id != 29 -- Hasanova Sabina
        and s.seller_id != 58 -- Stajer
        and s.seller_id != 33 -- Babayeva Ayna
        and s.seller_id != 28 -- Куратор КЦ
        and s.d_issue > date_trunc('week', current_date)
    group by s.seller_id
)

select
    u.id,
    u.last_name ||' '|| u.first_name as title
from base_user u
inner join sales on sales.seller_id = u.id
order by sales.cnt desc
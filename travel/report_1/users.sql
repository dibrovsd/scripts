with sales as (
    select
        s.seller_id,
        count(1) as cnt
    from reports.base_sales s
    where s.d_issue >= date_trunc('week', current_date)
    group by s.seller_id
)

select
    u.id,
    u.last_name ||' '|| u.first_name as title
from base_user u
inner join sales on sales.seller_id = u.id
order by sales.cnt desc
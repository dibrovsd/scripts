with sales as (
    select count(1) as cnt_sales
    from reports.base_osago s
    where s.channel_root_id = 9
        and s.seller_id != 51 -- fatima.huseynova
        and s.d_issue > current_date
)

select
    sales.cnt_sales,
    case
        when sales.cnt_sales >= plan.plan_day then 0
        else plan.plan_day - sales.cnt_sales
    end as plan,
    'Продано '|| sales.cnt_sales ||' из '|| plan.plan_day as category
from sales
cross join ({{datasets.settings.sql}}) plan

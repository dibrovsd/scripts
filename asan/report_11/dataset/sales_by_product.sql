select
    s.product as "Продукт",
    count(1) as "Продано"
from reports.base_sales s
where s.seller_territory_id = 9
    and s.seller_id != 51 -- fatima.huseynova
    and s.d_issue > current_date
group by s.product

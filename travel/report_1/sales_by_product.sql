select
    s.product as "Продукт",
    count(1) as "Продано"
from reports.base_sales s
where s.d_issue >= current_date
group by s.product
select
    s.product as "Продукт",
    count(1) as "Продано"
from reports.base_sales s
where s.channel_root_id = 9
    and s.seller_id != 51 -- fatima.huseynova
    and s.seller_id != 27 -- Shahsuvarova Lala
    and s.seller_id != 29 -- Hasanova Sabina
    and s.seller_id != 58 -- Stajer
    and s.seller_id != 33 -- Babayeva Ayna
    and s.seller_id != 28 -- Куратор КЦ
    and s.d_issue > current_date
group by s.product
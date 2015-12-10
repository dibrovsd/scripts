select
    s.product as "Продукт",
    count(1) as "Продано"
from reports.base_sales s
where s.channel_root_id = 9
    -- fatima.huseynova, Shahsuvarova Lala, Hasanova Sabina, Stajer, Babayeva Ayna, Куратор КЦ
    and s.seller_id not in (51, 27, 29, 58, 33, 28)
    and s.d_issue > current_date
group by s.product
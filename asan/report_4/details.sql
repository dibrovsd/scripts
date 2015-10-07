with t1 as (
    select * from ({{datasets.base.sql}}) as t
),

period as (
    {% if get.period|length == 10 %}
        select to_date([[get.period]], 'yyyy-mm-dd') as d_start,
               to_date([[get.period]], 'yyyy-mm-dd') + interval '1 day - 1 second' as d_end
    {% elif get.period|length == 7 %}
        select to_date([[get.period]], 'yyyy-mm') as d_start,
               to_date([[get.period]], 'yyyy-mm') + interval '1 month - 1 second' as d_end
    {% elif get.period|length == 4 %}
        select to_date([[get.period]], 'yyyy') as d_start,
               to_date([[get.period]], 'yyyy') + interval '1 year - 1 second' as d_end
    {% endif %}
)
select
    t1.d_start as "Дата выдачи",
    t1.insurer_fio as "ФИО",
    t1.s_kredit as "Сумма кредита",
    t1.currency as "Валюта",
    t1.s_insurance as "Сумма кредита (AZN)",
    t1.tem_months as "Срок кредита",
    t1.s_premium as "Премия",
    t1.s_ata as "ATA",
    t1.s_comission as "Odlar Yurdu (комиссия)",
    t1.s_cash as "Odlar Yurdu (наличка)",
    (t1.s_comission * 0.8) + t1.s_cash as "Odlar Yurdu (итого минус налог)",
    inc.summ as "Поступление",
    case
        when inc.summ > t1.s_premium then 'background-color: green; color: white;'
        when inc.summ < t1.s_premium then 'background-color: red; color: white;'
    end as incoming_attrs
from t1
cross join period
left join reports.rep4_incoming inc on inc.client like '%' || t1.id_client
where t1.d_start between period.d_start and period.d_end
order by t1.tem_months

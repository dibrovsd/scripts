{{datasets.src.sql}},

measures_gr as (
    select
        t.m_date,
        -- Осмотр
        count(case when t.measure = 'insp' then 1 end) as insp_cnt,
        count(case when t.measure = 'insp' and t.days_expire between 1 and 5 then 1 end) as insp_cnt_1_5,
        count(case when t.measure = 'insp' and t.days_expire between 6 and 10 then 1 end) as insp_cnt_6_10,
        count(case when t.measure = 'insp' and t.days_expire > 10 then 1 end) as insp_cnt_11,
        -- Ремонт
        count(case when t.measure = 'repair' then 1 end) as repair_cnt,
        count(case when t.measure = 'repair' and t.days_expire between 1 and 5 then 1 end) as repair_cnt_1_5,
        count(case when t.measure = 'repair' and t.days_expire between 6 and 10 then 1 end) as repair_cnt_6_10,
        count(case when t.measure = 'repair' and t.days_expire > 10 then 1 end) as repair_cnt_11,
        -- УУ
        count(case when t.measure = 'remote' then 1 end) as remote_cnt,
        count(case when t.measure = 'remote' and t.days_expire between 1 and 5 then 1 end) as remote_cnt_1_5,
        count(case when t.measure = 'remote' and t.days_expire between 6 and 10 then 1 end) as remote_cnt_6_10,
        count(case when t.measure = 'remote' and t.days_expire > 10 then 1 end) as remote_cnt_11
    from measures t
    group by t.m_date
)

select
    to_char(t.m_date, 'dd.mm.yyyy') as m_date,
    t.insp_cnt,
    t.insp_cnt_1_5,
    t.insp_cnt_6_10,
    t.insp_cnt_11,
    t.repair_cnt,
    t.repair_cnt_1_5,
    t.repair_cnt_6_10,
    t.repair_cnt_11,
    t.remote_cnt,
    t.remote_cnt_1_5,
    t.remote_cnt_6_10,
    t.remote_cnt_11,
    t.m_date as order_val,
    case
        when t.m_date = current_date then 'class="current_date"'
        else ''
    end as row_attributes
from measures_gr t

union all

select
    to_char(t.m_date, 'yyyy_mm') as m_date,
    sum(t.insp_cnt),
    sum(t.insp_cnt_1_5),
    sum(t.insp_cnt_6_10),
    sum(t.insp_cnt_11),
    sum(t.repair_cnt),
    sum(t.repair_cnt_1_5),
    sum(t.repair_cnt_6_10),
    sum(t.repair_cnt_11),
    sum(t.remote_cnt),
    sum(t.remote_cnt_1_5),
    sum(t.remote_cnt_6_10),
    sum(t.remote_cnt_11),
    max(t.m_date) + interval '1 second' as order_val,
    'class="row_summary"' as row_attributes
from measures_gr t
group by to_char(t.m_date, 'yyyy_mm')

union all

select
    'Итого просроченные' as m_date,
    sum(t.insp_cnt),
    sum(t.insp_cnt_1_5),
    sum(t.insp_cnt_6_10),
    sum(t.insp_cnt_11),
    sum(t.repair_cnt),
    sum(t.repair_cnt_1_5),
    sum(t.repair_cnt_6_10),
    sum(t.repair_cnt_11),
    sum(t.remote_cnt),
    sum(t.remote_cnt_1_5),
    sum(t.remote_cnt_6_10),
    sum(t.remote_cnt_11),
    null as order_val,
    'class="row_summary"' as row_attributes
from measures_gr t
where t.m_date < current_date

order by order_val nulls last

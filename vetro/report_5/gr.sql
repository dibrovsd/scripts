{{datasets.src.sql}},

base_gr as (
    select
        op.action,
        op.region,
        op.glass_type,
        --
        count(case when op.m = 'send_to_ins' then days_repair end) as days_repair_cnt,
        sum(case when op.m = 'send_to_ins' then days_repair end) as days_repair_sum,
        --
        count(case when op.m = 'send_to_ins' and days_repair between 0 and 4 then 1 end) as days_repair_cnt_0_4,
        count(case when op.m = 'send_to_ins' and days_repair between 0 and 9 then 1 end) as days_repair_cnt_0_9,
        count(case when op.m = 'send_to_ins' and days_repair between 0 and 14 then 1 end) as days_repair_cnt_0_14,
        count(case when op.m = 'send_to_ins' and days_repair between 0 and 24 then 1 end) as days_repair_cnt_0_24,
        count(case when op.m = 'send_to_ins' and days_repair >= 24 then 1 end) as days_repair_cnt_25,
        --
        count(case when op.m = 'pay' then days_documents end) as days_documents_cnt,
        sum(case when op.m = 'pay' then days_documents end) as days_documents_sum,
        --
        count(case when op.m = 'pay' then days_payment end) as days_payment_cnt,
        sum(case when op.m = 'pay' then days_payment end) as days_payment_sum,
        --
        count(case when op.m = 'pay' then days_summary end) as days_summary_cnt,
        sum(case when op.m = 'pay' then days_summary end) as days_summary_sum
    from operations op
    group by op.action, op.region, op.glass_type
),

-- Итоги регионов
cumul_1 as (
    select
        t.action,
        t.region,
        t.glass_type,
        t.days_repair_cnt,
        t.days_repair_sum,
        --
        t.days_repair_cnt_0_4,
        t.days_repair_cnt_0_9,
        t.days_repair_cnt_0_14,
        t.days_repair_cnt_0_24,
        t.days_repair_cnt_25,
        --
        t.days_documents_cnt,
        t.days_documents_sum,
        --
        t.days_payment_cnt,
        t.days_payment_sum,
        --
        t.days_summary_cnt,
        t.days_summary_sum

    from base_gr t

    union all

    select
        t.action,
        'Итого' as region,
        t.glass_type,
        sum(t.days_repair_cnt) as days_repair_cnt,
        sum(t.days_repair_sum) as days_repair_sum,
        --
        sum(t.days_repair_cnt_0_4) as days_repair_cnt_0_4,
        sum(t.days_repair_cnt_0_9) as days_repair_cnt_0_9,
        sum(t.days_repair_cnt_0_14) as days_repair_cnt_0_14,
        sum(t.days_repair_cnt_0_24) as days_repair_cnt_0_24,
        sum(t.days_repair_cnt_25) as days_repair_cnt_25,
        --
        sum(t.days_documents_cnt) as days_documents_cnt,
        sum(t.days_documents_sum) as days_documents_sum,
        --
        sum(t.days_payment_cnt) as days_payment_cnt,
        sum(t.days_payment_sum) as days_payment_sum,
        --
        sum(t.days_summary_cnt) as days_summary_cnt,
        sum(t.days_summary_sum) as days_summary_sum
    from base_gr t
    group by t.action, t.glass_type
),

-- Итоги типов стекол
cumul_2 as (
    select
        t.action,
        t.region,
        t.glass_type,
        t.days_repair_cnt,
        t.days_repair_sum,
        --
        t.days_repair_cnt_0_4,
        t.days_repair_cnt_0_9,
        t.days_repair_cnt_0_14,
        t.days_repair_cnt_0_24,
        t.days_repair_cnt_25,
        --
        t.days_documents_cnt,
        t.days_documents_sum,
        --
        t.days_payment_cnt,
        t.days_payment_sum,
        --
        t.days_summary_cnt,
        t.days_summary_sum

    from cumul_1 t

    union all

    select
        t.action,
        t.region,
        'Итого' as glass_type,
        sum(t.days_repair_cnt) as days_repair_cnt,
        sum(t.days_repair_sum) as days_repair_sum,
        --
        sum(t.days_repair_cnt_0_4) as days_repair_cnt_0_4,
        sum(t.days_repair_cnt_0_9) as days_repair_cnt_0_9,
        sum(t.days_repair_cnt_0_14) as days_repair_cnt_0_14,
        sum(t.days_repair_cnt_0_24) as days_repair_cnt_0_24,
        sum(t.days_repair_cnt_25) as days_repair_cnt_25,
        --
        sum(t.days_documents_cnt) as days_documents_cnt,
        sum(t.days_documents_sum) as days_documents_sum,
        --
        sum(t.days_payment_cnt) as days_payment_cnt,
        sum(t.days_payment_sum) as days_payment_sum,
        --
        sum(t.days_summary_cnt) as days_summary_cnt,
        sum(t.days_summary_sum) as days_summary_sum
    from cumul_1 t
    where t.action = 'Замена'
    group by t.action, t.region
),

-- Итоги действий
cumul_3 as (
    select
        t.action,
        t.region,
        t.glass_type,
        t.days_repair_cnt,
        t.days_repair_sum,
        --
        t.days_repair_cnt_0_4,
        t.days_repair_cnt_0_9,
        t.days_repair_cnt_0_14,
        t.days_repair_cnt_0_24,
        t.days_repair_cnt_25,
        --
        t.days_documents_cnt,
        t.days_documents_sum,
        --
        t.days_payment_cnt,
        t.days_payment_sum,
        --
        t.days_summary_cnt,
        t.days_summary_sum

    from cumul_2 t

    union all

    select
        'Итого' as action,
        t.region,
        t.glass_type,
        sum(t.days_repair_cnt) as days_repair_cnt,
        sum(t.days_repair_sum) as days_repair_sum,
        --
        sum(t.days_repair_cnt_0_4) as days_repair_cnt_0_4,
        sum(t.days_repair_cnt_0_9) as days_repair_cnt_0_9,
        sum(t.days_repair_cnt_0_14) as days_repair_cnt_0_14,
        sum(t.days_repair_cnt_0_24) as days_repair_cnt_0_24,
        sum(t.days_repair_cnt_25) as days_repair_cnt_25,
        --
        sum(t.days_documents_cnt) as days_documents_cnt,
        sum(t.days_documents_sum) as days_documents_sum,
        --
        sum(t.days_payment_cnt) as days_payment_cnt,
        sum(t.days_payment_sum) as days_payment_sum,
        --
        sum(t.days_summary_cnt) as days_summary_cnt,
        sum(t.days_summary_sum) as days_summary_sum
    from cumul_2 t
    where t.glass_type = 'Итого'
    group by t.glass_type, t.region
),

-- Финальный расчет
cumul as (
    select
        t.action,
        t.region,
        t.glass_type,
        --
        round(f_division(t.days_repair_sum, t.days_repair_cnt)) as days_repair_avg,
        --
        round(f_division(t.days_repair_cnt_0_4, t.days_repair_cnt) * 100) as days_repair_ratio_0_4,
        round(f_division(t.days_repair_cnt_0_9, t.days_repair_cnt) * 100) as days_repair_ratio_0_9,
        round(f_division(t.days_repair_cnt_0_14, t.days_repair_cnt) * 100) as days_repair_ratio_0_14,
        round(f_division(t.days_repair_cnt_0_24, t.days_repair_cnt) * 100) as days_repair_ratio_0_24,
        round(f_division(t.days_repair_cnt_25, t.days_repair_cnt) * 100) as days_repair_ratio_25,
        --
        round(f_division(t.days_documents_sum, t.days_documents_cnt)) as days_documents_avg,
        round(f_division(t.days_payment_sum, t.days_payment_cnt)) as days_payment_avg,
        round(f_division(t.days_summary_sum, t.days_summary_cnt)) as days_summary_avg

    from cumul_3 t
),

struct as (
    select 'ЗАМЕНА' as title,
        'background-color: #DCDCDC; font-weight: bold;' as row_style,
        1 as n_order,
        null as glass_type,
        null as action,
        null as region

    union all

    select 'Москва' as title,
        'background-color: #FFDAB9; font-weight: bold;' as row_style,
        2 as n_order,
        null as glass_type,
        null as action,
        null as region

    union all

    select 'Не оригинальное стекло' as title,
        '' as row_style,
        4 as n_order,
        'Не оригинальное' as glass_type,
        'Замена' as action,
        'Москва' as region

    union all

    select 'Оригинальное стекло' as title,
        '' as row_style,
        5 as n_order,
        'Оригинальное' as glass_type,
        'Замена' as action,
        'Москва' as region

    union all

    select 'Все стекла' as title,
        '' as row_style,
        6 as n_order,
        'Итого' as glass_type,
        'Замена' as action,
        'Москва' as region

        union all

    select 'Регионы' as title,
        'background-color: #FFDAB9; font-weight: bold;' as row_style,
        7 as n_order,
        null as glass_type,
        null as action,
        null as region

    union all

    select 'Не оригинальное стекло' as title,
        '' as row_style,
        8 as n_order,
        'Не оригинальное' as glass_type,
        'Замена' as action,
        'Регион' as region

    union all

    select 'Оригинальное стекло' as title,
        '' as row_style,
        9 as n_order,
        'Оригинальное' as glass_type,
        'Замена' as action,
        'Регион' as region

    union all

    select 'Все стекла' as title,
        '' as row_style,
        10 as n_order,
        'Итого' as glass_type,
        'Замена' as action,
        'Регион' as region

    union all

    select 'Москва/Регионы' as title,
        'background-color: #FFDAB9; font-weight: bold;' as row_style,
        11 as n_order,
        null as glass_type,
        null as action,
        null as region

    union all

    select 'Не оригинальное стекло' as title,
        '' as row_style,
        12 as n_order,
        'Не оригинальное' as glass_type,
        'Замена' as action,
        'Итого' as region

    union all

    select 'Оригинальное стекло' as title,
        '' as row_style,
        13 as n_order,
        'Оригинальное' as glass_type,
        'Замена' as action,
        'Итого' as region

    union all

    select 'Все стекла' as title,
        '' as row_style,
        14 as n_order,
        'Итого' as glass_type,
        'Замена' as action,
        'Итого' as region

    union all

    select 'РЕМОНТ' as title,
        'background-color: #DCDCDC; font-weight: bold;' as row_style,
        15 as n_order,
        null as glass_type,
        null as action,
        null as region

    union all

    select 'Москва' as title,
        '' as row_style,
        16 as n_order,
        'Итого' as glass_type,
        'Ремонт' as action,
        'Москва' as region

    union all

    select 'Регион' as title,
        '' as row_style,
        17 as n_order,
        'Итого' as glass_type,
        'Ремонт' as action,
        'Регион' as region

    union all

    select 'Москва/Регионы' as title,
        '' as row_style,
        18 as n_order,
        'Итого' as glass_type,
        'Ремонт' as action,
        'Итого' as region

    union all

    select 'ИТОГО ЗАМЕНА/РЕМОНТ' as title,
        'background-color: #DCDCDC; font-weight: bold;' as row_style,
        19 as n_order,
        null as glass_type,
        null as action,
        null as region

    union all

    select 'Москва' as title,
        '' as row_style,
        20 as n_order,
        'Итого' as glass_type,
        'Итого' as action,
        'Москва' as region

    union all

    select 'Регион' as title,
        '' as row_style,
        21 as n_order,
        'Итого' as glass_type,
        'Итого' as action,
        'Регион' as region

    union all

    select 'Москва/Регионы' as title,
        '' as row_style,
        22 as n_order,
        'Итого' as glass_type,
        'Итого' as action,
        'Итого' as region
)

select
    s.title,
    --
    c.days_repair_avg,
    --
    c.days_repair_ratio_0_4,
    c.days_repair_ratio_0_9,
    c.days_repair_ratio_0_14,
    c.days_repair_ratio_0_24,
    c.days_repair_ratio_25,
    --
    c.days_documents_avg,
    c.days_payment_avg,
    c.days_summary_avg,
    --
    s.row_style,
    s.action,
    s.glass_type,
    s.region
from struct s
left join cumul c on c.action = s.action
                  and c.region = s.region
                  and c.glass_type = s.glass_type
order by s.n_order

{{datasets.src.sql}},

base_gr as (
    select
        op.action,
        op.region,
        op.glass_type,
        --
        count(1) as cnt,
        count(days) as days_cnt,
        sum(days) as days_sum,
        --
        count(case when days between 0 and 4 then 1 end) as days_cnt_0_4,
        count(case when days between 0 and 9 then 1 end) as days_cnt_0_9,
        count(case when days between 0 and 14 then 1 end) as days_cnt_0_14,
        count(case when days between 0 and 24 then 1 end) as days_cnt_0_24,
        count(case when days >= 25 then 1 end) as days_cnt_25
    from operations op
    where op.m = 'full_process'
    group by op.action, op.region, op.glass_type
),

-- Итоги регионов
cumul_1 as (
    select
        t.action,
        t.region,
        t.glass_type,
        t.cnt,
        t.days_cnt,
        t.days_sum,
        t.days_cnt_0_4,
        t.days_cnt_0_9,
        t.days_cnt_0_14,
        t.days_cnt_0_24,
        t.days_cnt_25
    from base_gr t

    union all

    select
        t.action,
        'Итого' as region,
        t.glass_type,
        sum(t.cnt) as cnt,
        sum(t.days_cnt) as days_cnt,
        sum(t.days_sum) as days_sum,
        --
        sum(t.days_cnt_0_4) as days_cnt_0_4,
        sum(t.days_cnt_0_9) as days_cnt_0_9,
        sum(t.days_cnt_0_14) as days_cnt_0_14,
        sum(t.days_cnt_0_24) as days_cnt_0_24,
        sum(t.days_cnt_25) as days_cnt_25
    from base_gr t
    group by t.action, t.glass_type
),

-- Итоги типов стекол
cumul_2 as (
    select
        t.action,
        t.region,
        t.glass_type,
        t.cnt,
        t.days_cnt,
        t.days_sum,
        t.days_cnt_0_4,
        t.days_cnt_0_9,
        t.days_cnt_0_14,
        t.days_cnt_0_24,
        t.days_cnt_25
    from cumul_1 t

    union all

    select
        t.action,
        t.region,
        'Итого' as glass_type,
        sum(t.cnt) as cnt,
        sum(t.days_cnt) as days_cnt,
        sum(t.days_sum) as days_sum,
        sum(t.days_cnt_0_4) as days_cnt_0_4,
        sum(t.days_cnt_0_9) as days_cnt_0_9,
        sum(t.days_cnt_0_14) as days_cnt_0_14,
        sum(t.days_cnt_0_24) as days_cnt_0_24,
        sum(t.days_cnt_25) as days_cnt_25
    from cumul_1 t
    -- where t.action = 'Замена'
    group by t.action, t.region
),

-- Итоги действий
cumul_3 as (
    select
        t.action,
        t.region,
        t.glass_type,
        t.cnt,
        t.days_cnt,
        t.days_sum,
        t.days_cnt_0_4,
        t.days_cnt_0_9,
        t.days_cnt_0_14,
        t.days_cnt_0_24,
        t.days_cnt_25
    from cumul_2 t

    union all

    select
        'Итого' as action,
        t.region,
        t.glass_type,
        sum(t.cnt) as cnt,
        sum(t.days_cnt) as days_cnt,
        sum(t.days_sum) as days_sum,
        sum(t.days_cnt_0_4) as days_cnt_0_4,
        sum(t.days_cnt_0_9) as days_cnt_0_9,
        sum(t.days_cnt_0_14) as days_cnt_0_14,
        sum(t.days_cnt_0_24) as days_cnt_0_24,
        sum(t.days_cnt_25) as days_cnt_25
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
        t.cnt,
        t.days_cnt,
        round(f_division(t.days_sum, t.days_cnt)) as days_avg,
        round(f_division(t.days_cnt_0_4, t.days_cnt) * 100) as days_ratio_0_4,
        round(f_division(t.days_cnt_0_9, t.days_cnt) * 100) as days_ratio_0_9,
        round(f_division(t.days_cnt_0_14, t.days_cnt) * 100) as days_ratio_0_14,
        round(f_division(t.days_cnt_0_24, t.days_cnt) * 100) as days_ratio_0_24,
        round(f_division(t.days_cnt_25, t.days_cnt) * 100) as days_ratio_25
    from cumul_3 t
)

-- Нанизываем на структуру
select
    s.title,
    --
    c.cnt,
    c.days_avg,
    c.days_ratio_0_4,
    c.days_ratio_0_9,
    c.days_ratio_0_14,
    c.days_ratio_0_24,
    c.days_ratio_25,
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

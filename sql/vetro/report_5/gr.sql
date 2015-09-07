with params as (
    select
        [[env.period.0]] as d_start,
        [[env.period.1]] as d_end,
        [[env.city_auto_host]]::integer as city_auto_host,
        [[env.direction_stoa]]::integer as direction_stoa,
        [[env.stoa_company]]::integer as stoa_company,
        [[env.curator]]::integer as curator,
        [[env.inscompany]]::integer as inscompany

        -- 0 as city_auto_host,
        -- 0 as direction_stoa,
        -- 0 as responsible,
        -- to_date('01.07.2015', 'dd.mm.yyyy') as d_start,
        -- to_date('01.08.2015', 'dd.mm.yyyy') - interval '1 second' as d_end
),

-- База
base as (
    select
        d.damages_action as action,
        case
            when d.city_auto_host_id = 12
                then 'Москва'
            else 'Регион'
        end as region,
        case
            when d.damages_action = 'Замена'
                then d.replace_glass_glass_type
            else 'Итого'
        end as glass_type,
        round(d.repair_date_real::date - d.direction_get_date::date) as days_repair, -- До ремонта
        round(d.d_documents_send::date - d.repair_date_real::date) as days_documents, -- До передачи документов в СК
        round(d.pay_date::date - d.d_documents_send::date) as days_payment, -- До оплаты
        round(d.pay_date::date - d.direction_get_date::date) as days_summary, -- Полный цикл
        d.measure_date
    from (
        select d.*,
            d.{{env.period_date}} as measure_date
        from reports.v_document d
    ) d
    cross join params
    where (params.city_auto_host = 0 or d.city_auto_host_id = params.city_auto_host)
      and (params.direction_stoa = 0 or d.stoa_id = params.direction_stoa)
      and (params.stoa_company = 0 or d.stoa_company_id = params.stoa_company)
      and (params.curator = 0 or d.curator_id = params.curator)
      and (params.inscompany = 0 or d.inscompany_id = params.inscompany)
      and d.measure_date between params.d_start and params.d_end
),

-- Группируем
base_gr as (
    select
        t.action,
        t.region,
        t.glass_type,
        --
        count(days_repair) as days_repair_cnt,
        sum(days_repair) as days_repair_sum,
        --
        count(case when days_repair between 0 and 4 then 1 end) as days_repair_cnt_0_4,
        count(case when days_repair between 0 and 9 then 1 end) as days_repair_cnt_0_9,
        count(case when days_repair between 0 and 14 then 1 end) as days_repair_cnt_0_14,
        count(case when days_repair between 0 and 24 then 1 end) as days_repair_cnt_0_24,
        count(case when days_repair >= 24 then 1 end) as days_repair_cnt_25,
        --
        count(days_documents) as days_documents_cnt,
        sum(days_documents) as days_documents_sum,
        --
        count(days_payment) as days_payment_cnt,
        sum(days_payment) as days_payment_sum,
        --
        count(days_summary) as days_summary_cnt,
        sum(days_summary) as days_summary_sum
    from base t
    group by t.action, t.region, t.glass_type
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

-- Итоги стекол
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

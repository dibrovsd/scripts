with params as (
    select
        [[env.city]]::integer as city,
        [[env.stoa_company]]::integer as stoa_company,
        [[env.inscompany]]::integer as inscompany

        -- to_date('01.07.2015', 'dd.mm.yyyy') as d_start,
        -- to_date('01.08.2015', 'dd.mm.yyyy') - interval '1 second' as d_end,
        -- 0 as city,
        -- 0 as stoa_company,
        -- 0 as responsible,
        -- 0 as inscompany
),

-- База
base as (
    select
        case
            when d.city_auto_host_id = 12 then 'Москва'
            else 'Регион'
        end as region,
        d.s_repair_all as sum,
        (current_date - d.d_documents_send::date) as days,
        f_workdays(d.d_documents_send::date, current_date) as workdays,
        d.inscompany_id
    from reports.v_document d
    cross join params
    where d.d_documents_send is not null
      and d.pay_date is null
      and (params.city = 0 or d.city_auto_host_id = params.city)
      and (params.stoa_company = 0 or d.stoa_company_id = params.stoa_company)
      and (params.inscompany = 0 or d.inscompany_id = params.inscompany)
),

-- Группируем по базовому уровню
base_gr as (
    select
        t.region,
        t.inscompany_id,
        count(1) as cnt,
        sum(t.sum) as sum,
        --
        count(case when t.workdays <= 15 then 1 end) as cnt_lte_15,
        sum(case when t.workdays <= 15 then t.sum end) as sum_lte_15,
        --
        count(case when t.workdays > 15 then 1 end) as cnt_gt_15,
        sum(case when t.workdays > 15 then t.sum end) as sum_gt_15,
        --
        count(case when t.days > 30 then 1 end) as cnt_gt_30,
        sum(case when t.days > 30 then t.sum end) as sum_gt_30,
        --
        count(case when t.days > 60 then 1 end) as cnt_gt_60,
        sum(case when t.days > 60 then t.sum end) as sum_gt_60
    from base t
    group by t.region, t.inscompany_id
),

-- Итоги по регионам
cumul1 as (
    select
        t.region,
        t.inscompany_id,
        t.cnt,
        t.sum,
        t.cnt_lte_15,
        t.sum_lte_15,
        t.cnt_gt_15,
        t.sum_gt_15,
        t.cnt_gt_30,
        t.sum_gt_30,
        t.cnt_gt_60,
        t.sum_gt_60
    from base_gr t
    union all
    select
        'Итого' as region,
        t.inscompany_id,
        sum(t.cnt) as cnt,
        sum(t.sum) as sum,
        sum(t.cnt_lte_15) as cnt_lte_15,
        sum(t.sum_lte_15) as sum_lte_15,
        sum(t.cnt_gt_15) as cnt_gt_15,
        sum(t.sum_gt_15) as sum_gt_15,
        sum(t.cnt_gt_30) as cnt_gt_30,
        sum(t.sum_gt_30) as sum_gt_30,
        sum(t.cnt_gt_60) as cnt_gt_60,
        sum(t.sum_gt_60) as sum_gt_60
    from base_gr t
    group by t.inscompany_id
),

-- Итоги по компаниям
cumul2 as (
    select
        t.region,
        t.inscompany_id,
        t.cnt,
        t.sum,
        t.cnt_lte_15,
        t.sum_lte_15,
        t.cnt_gt_15,
        t.sum_gt_15,
        t.cnt_gt_30,
        t.sum_gt_30,
        t.cnt_gt_60,
        t.sum_gt_60
    from cumul1 t
    union all
    select
        t.region,
        -1 as inscompany_id,
        sum(t.cnt) as cnt,
        sum(t.sum) as sum,
        sum(t.cnt_lte_15) as cnt_lte_15,
        sum(t.sum_lte_15) as sum_lte_15,
        sum(t.cnt_gt_15) as cnt_gt_15,
        sum(t.sum_gt_15) as sum_gt_15,
        sum(t.cnt_gt_30) as cnt_gt_30,
        sum(t.sum_gt_30) as sum_gt_30,
        sum(t.cnt_gt_60) as cnt_gt_60,
        sum(t.sum_gt_60) as sum_gt_60
    from cumul1 t
    group by t.region
),

structure_ as (
    -------------
    select 'РЕСО-Гарантия' as title,
        'background-color: #FFDAB9; font-weight: bold;' as row_style,
        null as region,
        null as inscompany_id

    union all

    select 'Москва' as title,
        '' as row_style,
        'Москва' as region,
        3 as inscompany_id

    union all

    select 'Регионы' as title,
        '' as row_style,
        'Регион' as region,
        3 as inscompany_id

    union all

    select 'Итого по Ресо-Гарантия' as title,
        '' as row_style,
        'Итого' as region,
        3 as inscompany_id

    union all
    -------------
    select 'СОГЛАСИЕ' as title,
        'background-color: #FFDAB9; font-weight: bold;' as row_style,
        null as region,
        null as inscompany_id

    union all

    select 'Москва' as title,
        '' as row_style,
        'Москва' as region,
        2 as inscompany_id

    union all

    select 'Регионы' as title,
        '' as row_style,
        'Регион' as region,
        2 as inscompany_id

    union all

    select 'Итого по Согласие' as title,
        '' as row_style,
        'Итого' as region,
        2 as inscompany_id

    union all
    -------------
    select 'СОГАЗ' as title,
        'background-color: #FFDAB9; font-weight: bold;' as row_style,
        null as region,
        null as inscompany_id

    union all

    select 'Москва' as title,
        '' as row_style,
        'Москва' as region,
        1 as inscompany_id

    union all

    select 'Регионы' as title,
        '' as row_style,
        'Регион' as region,
        1 as inscompany_id

    union all

    select 'Итого по СОГАЗ' as title,
        '' as row_style,
        'Итого' as region,
        1 as inscompany_id

    union all


    -------------
    select 'Югория' as title,
        'background-color: #FFDAB9; font-weight: bold;' as row_style,
        null as region,
        null as inscompany_id

    union all

    select 'Москва' as title,
        '' as row_style,
        'Москва' as region,
        8 as inscompany_id

    union all

    select 'Регионы' as title,
        '' as row_style,
        'Регион' as region,
        8 as inscompany_id

    union all

    select 'Итого по Югории' as title,
        '' as row_style,
        'Итого' as region,
        8 as inscompany_id

    union all

    -------------
    select 'Альфа Страхование' as title,
        'background-color: #FFDAB9; font-weight: bold;' as row_style,
        null as region,
        null as inscompany_id

    union all

    select 'Москва' as title,
        '' as row_style,
        'Москва' as region,
        7 as inscompany_id

    union all

    select 'Регионы' as title,
        '' as row_style,
        'Регион' as region,
        7 as inscompany_id

    union all

    select 'Итого по Альфа Страхованию' as title,
        '' as row_style,
        'Итого' as region,
        7 as inscompany_id

    union all

    -------------
    select 'Зетта' as title,
        'background-color: #FFDAB9; font-weight: bold;' as row_style,
        null as region,
        null as inscompany_id

    union all

    select 'Москва' as title,
        '' as row_style,
        'Москва' as region,
        6 as inscompany_id

    union all

    select 'Регионы' as title,
        '' as row_style,
        'Регион' as region,
        6 as inscompany_id

    union all

    select 'Итого по Зетта' as title,
        '' as row_style,
        'Итого' as region,
        6 as inscompany_id

    union all

    -------------
    select 'Тинькофф' as title,
        'background-color: #FFDAB9; font-weight: bold;' as row_style,
        null as region,
        null as inscompany_id

    union all

    select 'Москва' as title,
        '' as row_style,
        'Москва' as region,
        4 as inscompany_id

    union all

    select 'Регионы' as title,
        '' as row_style,
        'Регион' as region,
        4 as inscompany_id

    union all

    select 'Итого по Тинькофф' as title,
        '' as row_style,
        'Итого' as region,
        4 as inscompany_id

    union all

    -------------
    select 'Итого' as title,
        'background-color: #DCDCDC; font-weight: bold;' as row_style,
        null as region,
        null as inscompany_id

    union all

    select 'Москва' as title,
        '' as row_style,
        'Москва' as region,
        -1 as inscompany_id

    union all

    select 'Регионы' as title,
        '' as row_style,
        'Регион' as region,
        -1 as inscompany_id

    union all

    select 'Итого' as title,
        '' as row_style,
        'Итого' as region,
        -1 as inscompany_id
),

structure as (
    select
        t.*,
        row_number() over() as n_order
    from structure_ t
)

select
    s.title,
    d.cnt,
    d.sum,
    d.cnt_lte_15,
    d.sum_lte_15,
    d.cnt_gt_15,
    d.sum_gt_15,
    d.cnt_gt_30,
    d.sum_gt_30,
    d.cnt_gt_60,
    d.sum_gt_60,
    --
    s.row_style,
    s.region,
    s.inscompany_id
from structure s
left join cumul2 d on d.region = s.region
                   and d.inscompany_id = s.inscompany_id
order by s.n_order

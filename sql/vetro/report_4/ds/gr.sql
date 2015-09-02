with params as (
    select
        [[env.period.0]] as d_from,
        [[env.period.1]] as d_to,
        [[env.city_auto_host]]::integer as city_auto_host,
        [[env.direction_stoa]]::integer as direction_stoa,
        [[env.inscompany]]::integer as inscompany

        -- 0 as city_auto_host,
        -- 0 as direction_stoa,
        -- 0 as responsible,
        -- to_date('01.07.2015', 'dd.mm.yyyy') as d_from,
        -- to_date('01.08.2015', 'dd.mm.yyyy') - interval '1 second' as d_to
),

base as (
    select
        case
            when d.city_auto_host_id = 12
                then 'Москва'
            else 'Регион'
        end as region,
        d.damages_action as action,
        d.replace_glass_glass_type as glass_type,
        d.s_repair_all as measure_sum,
        d.{{env.period_date}} as measure_date
        -- d.repair_date_real as measure_date
    from reports.v_document d
    cross join params
    where (params.city_auto_host = 0 or d.city_auto_host_id = params.city_auto_host)
      and (params.direction_stoa = 0 or d.stoa_id = params.direction_stoa)
      and (params.inscompany = 0 or d.inscompany_id = params.inscompany)

),

periods as (
    {{datasets.periods.sql}}
    -- select
    --     '2015_06'::varchar as title,
    --     to_date('01.06.2015', 'dd.mm.yyyy') as d_start,
    --     to_date('30.06.2015', 'dd.mm.yyyy') as d_end
),

-- База
base_gr as (
    select
        periods.title as period,
        b.region,
        b.action,
        b.glass_type,
        count(1) as cnt,
        sum(measure_sum) as measure_sum
    from base b
    inner join periods on b.measure_date between periods.d_start and periods.d_end
    where measure_sum is not null
      and measure_date is not null
    group by periods.title,
        b.region,
        b.action,
        b.glass_type
),

base_gr_cum as (
    select period,
        region,
        action,
        glass_type,
        cnt,
        measure_sum
    from base_gr

    union all

    select period,
        'ИТОГО (Москва и Регионы)' as region,
        action,
        glass_type,
        sum(cnt) as cnt,
        sum(measure_sum) as measure_sum
    from base_gr
    group by period, action, glass_type
),

calc as (

    select t.period,
        t.region,
        sum(measure_sum) as all_sum,
        sum(cnt) as all_cnt,
        -- Замена
        sum(case when action = 'Замена' then cnt end) as replace_cnt,
        -- Замена (Не оригинальное)
        sum(case when action = 'Замена' and glass_type = 'Не оригинальное' then cnt end) as replace_not_original_cnt,
        sum(case when action = 'Замена' and glass_type = 'Не оригинальное' then measure_sum end) as replace_not_original_sum,
        -- Замена (Оригинальное)
        sum(case when action = 'Замена' and glass_type = 'Оригинальное' then cnt end) as replace_original_cnt,
        sum(case when action = 'Замена' and glass_type = 'Оригинальное' then measure_sum end) as replace_original_sum,
        -- Ремонт
        sum(case when action = 'Ремонт' then cnt end) as repair_cnt,
        sum(case when action = 'Ремонт' then measure_sum end) as repair_sum
    from base_gr_cum t
    group by t.period, t.region
),

calc1 as (
    select
        calc.period,
        calc.region,
        --
        round(calc.all_sum / calc.all_cnt) as all_avg,
        calc.all_cnt,
        --
        round(calc.replace_not_original_sum / calc.replace_not_original_cnt) as replace_not_original_avg,
        calc.replace_not_original_cnt,
        round(calc.replace_not_original_cnt / calc.replace_cnt * 100) as replace_not_original_ratio,
        --
        round(calc.replace_original_sum / calc.replace_original_cnt) as replace_original_avg,
        calc.replace_original_cnt,
        round(calc.replace_original_cnt / calc.replace_cnt * 100) as replace_original_ratio,
        --
        round(calc.repair_sum / calc.repair_cnt) as repair_avg,
        calc.repair_cnt,
        round(calc.repair_cnt / calc.all_cnt * 100) as repair_ratio
    from calc
),

calc2 as (
    select
        t.period,
        t.region,
        unnest(array[0,1,2,3,4,5,6,7,8,9,10,11]) as n_order,
        unnest(array[null,
                     'Средний убыток (ИТОГО)',
                     'Количество',
                     'Средний убыток (Неоригинальное стекло)',
                     'Количество',
                     'Доля неоригинальных стекол (%)',
                     'Средний убыток (Оригинальное стекло)',
                     'Количество',
                     'Доля оригинальных стекол (%)',
                     'Средний убыток (Ремонт)',
                     'Количество',
                     'Доля замен (%)']) as title,
        unnest(array[null,
                     all_avg,
                     all_cnt,
                     replace_not_original_avg,
                     replace_not_original_cnt,
                     replace_not_original_ratio,
                     replace_original_avg,
                     replace_original_cnt,
                     replace_original_ratio,
                     repair_avg,
                     repair_cnt,
                     repair_ratio]) as val,
        unnest(array['background-color: #DCDCDC; font-weight: bold;',
                     'background-color: #FFDAB9; font-weight: bold;',
                     null,
                     null,
                     null,
                     null,
                     null,
                     null,
                     null,
                     null,
                     null,
                     null])as row_style,
        unnest(array[null,
                     'all_avg',
                     'all_cnt',
                     'replace_not_original_avg',
                     'replace_not_original_cnt',
                     'replace_not_original_ratio',
                     'replace_original_avg',
                     'replace_original_cnt',
                     'replace_original_ratio',
                     'repair_avg',
                     'repair_cnt',
                     'repair_ratio']) as measure_id
    from calc1 t
)

select
    t.region,
    coalesce(t.title, t.region) as title,

    {% for row in datasets.periods.data %}
        max(case when t.period = '{{row.title}}' then t.val end) as "{{row.title}}",
    {% endfor %}
    -- max(case when t.period = '2015_06' then t.val end) as "2015_06",

    t.n_order,
    t.row_style,
    t.measure_id
from calc2 t
group by t.region, t.n_order, t.title, t.row_style, t.measure_id
order by t.region, t.n_order

with params as (
    select

    [[env.city_auto_host]]::integer as city_auto_host,
    [[env.direction_stoa]]::integer as direction_stoa,
    [[env.responsible]]::integer as responsible,
    [[env.curator]]::integer as curator


    -- 0 as city_auto_host,
    -- 0 as direction_stoa,
    -- 0 as responsible
),

documents as (
    select * from reports.v_document d
    cross join params
    where (params.city_auto_host = 0 or d.city_auto_host_id = params.city_auto_host)
      and (params.direction_stoa = 0 or d.stoa_id = params.direction_stoa)
      and (params.responsible = 0 or d.responsible_id = params.responsible)
      and (params.curator = 0 or d.curator_id = params.curator)

      {% if 'customer_service' in user_params.roles %}
         and d.curator_id = {{user.id}}

      {% elif 'stoa' in user_params.roles %}
         and d.stoa_id in ({{user.stations_ids|join:","}})

      {% endif %}
),

measures as (
    select
        'insp' as measure,
        d.inspection_date::date as m_date,
        current_date - d.inspection_date::date as days_expire
    from documents d
    where d.inspection_date is not null
      and d.inspection_date_real is null

    union all

    select
        'repair' as measure,
        d.repair_date::date as m_date,
        current_date - d.repair_date::date as days_expire
    from documents d
    where d.repair_date is not null
      and d.repair_date_real is null
),

measures_gr as (
    select
        t.m_date,
        -- Осмотр
        count(case when t.measure = 'insp' then 1 end) as insp_cnt,
        count(case when t.measure = 'insp' and t.days_expire between 0 and 5 then 1 end) as insp_cnt_1_5,
        count(case when t.measure = 'insp' and t.days_expire between 6 and 10 then 1 end) as insp_cnt_6_10,
        count(case when t.measure = 'insp' and t.days_expire > 10 then 1 end) as insp_cnt_11,
        -- Ремонт
        count(case when t.measure = 'repair' then 1 end) as repair_cnt,
        count(case when t.measure = 'repair' and t.days_expire between 0 and 5 then 1 end) as repair_cnt_1_5,
        count(case when t.measure = 'repair' and t.days_expire between 6 and 10 then 1 end) as repair_cnt_6_10,
        count(case when t.measure = 'repair' and t.days_expire > 10 then 1 end) as repair_cnt_11
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
    null as order_val,
    'class="row_summary"' as row_attributes
from measures_gr t
where t.m_date < current_date

order by order_val nulls last

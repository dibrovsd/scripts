with params as (
    select
         [[env.city_auto_host]]::integer as city_auto_host,
         [[env.direction_stoa]]::integer as direction_stoa,
         [[env.responsible]]::integer as curator

        -- 0 as city_auto_host,
        -- 0 as direction_stoa,
        -- 0 as responsible
),

documents as (
    select
        d.id,
        d.d_create::date as d_create,
        d.event_create::date as d_create_event,
        d.repair_date_real::date as repair_date_real,
        d.pay_date,
        d.pay_sum,
        d.state_id,
        d.inscompany_id
    from reports.v_document d
    cross join params
    where (params.city_auto_host = 0 or d.city_auto_host_id = params.city_auto_host)
      and (params.direction_stoa = 0 or d.stoa_id = params.direction_stoa)
      and (params.curator = 0 or d.curator_id = params.curator)

        {% if 'customer_service' in user_params.roles %}
           and d.curator_id = {{user.id}}

        {% elif 'stoa' in user_params.roles %}
           and d.stoa_id in ({{user.stations_ids|join:","}})
           -- "Ожидание решения СК о смене СТОА" и "Ожидание оплаты"
           and d.state_id not in (15, 11)

        {% endif %}
),

document_event as (
    select e.document_id,
        e.d_create::date as d_create,
        e.state_to_id,
        e.state_from_id,
        d.pay_date,
        d.pay_sum
    from reports.rep_1_last_events e
    inner join documents d on d.id = e.document_id

),

incoming as (
    select d.d_create,
           d.inscompany_id,
           count(1) as cnt
    from documents d
    group by d.d_create, d.inscompany_id
),

out_repair as (
    select d.repair_date_real as d_create,
           count(1) as cnt
    from documents d
    where d.repair_date_real is not null
    group by d.repair_date_real
),

-- На этап ожидания оплаты
out_wait_paymenet as (
    select e.d_create,
           count(1) as cnt
    from document_event e
    where e.state_to_id = 11 -- Ожидание оплаты
    group by e.d_create
),

-- архив_Оплата
-- Попали на этап "архив" через этап "ожидание оплаты"
-- и по которым заполнены поля "дата оплаты" и "сумма оплаты".
out_archive_payment as (
    select e.d_create,
           count(1) as cnt
    from document_event e
    where e.state_to_id = 12 -- Архив
      and e.state_from_id = 11 -- Ожидание оплаты
      and e.pay_date is not null
      and e.pay_sum is not null
    group by e.d_create
),

-- архив_Отказ
-- Попали на этап "архив"
-- НЕ:
-- через этап "ожидание оплаты"
-- и по которым заполнены поля "дата оплаты" и "сумма оплаты".
out_archive_reject as (
    select e.d_create,
           count(1) as cnt
    from document_event e
    where e.state_to_id = 12 -- Архив
      and not (
          e.state_from_id = 11 -- Ожидание оплаты
          and e.pay_date is not null
          and e.pay_sum is not null
      )
    group by e.d_create
),

combined as (
    select 'in' as measure,
        d_create,
        inscompany_id,
        cnt
    from incoming

    union all

    select 'in' as measure,
        d_create,
        null::integer as inscompany_id,
        sum(cnt) as cnt
    from incoming
    group by d_create

    union all

    select 'out_repair' as measure,
        d_create,
        null::integer,
        cnt
    from out_repair

    union all

    select 'out_wait_paymenet' as measure,
        d_create,
        null::integer,
        cnt
    from out_wait_paymenet

    union all

    select 'out_archive_payment' as measure,
        d_create,
        null::integer,
        cnt
    from out_archive_payment

    union all

    select 'out_archive_reject' as measure,
        d_create,
        null::integer,
        cnt
    from out_archive_reject

),

combined_cumul as (
    select
        to_char(d_create, 'yyyy-mm-dd') as d_create,
        measure,
        inscompany_id,
        cnt,
        d_create as d_max,
        d_create as d_min,
        1 as period_len,
        '' as row_attributes
    from combined
    where d_create >= date_trunc('month', current_date)

    union all

    select
        to_char(d_create, 'yyyy-mm') as d_create,
        measure,
        inscompany_id,
        sum(cnt) as cnt,
        max(d_create) as d_max,
        min(d_create) as d_min,
        3 as period_len,
        'class="row_summary"' as row_attributes
    from combined
    group by to_char(d_create, 'yyyy-mm'),
        to_char(d_create, 'yyyy'),
        measure,
        inscompany_id

    union all

    select
        to_char(d_create, 'yyyy') as d_create,
        measure,
        inscompany_id,
        sum(cnt) as cnt,
        max(d_create) as d_max,
        min(d_create) as d_min,
        5 as period_len,
        'class="row_summary"' as row_attributes
    from combined
    group by to_char(d_create, 'yyyy'),
        measure,
        inscompany_id
)


select
    t.d_create as "Дата",
    -- Вход
    {% for row in datasets.inscompany.data %}
    sum(case when t.measure = 'in' and t.inscompany_id = {{row.id}} then cnt end) as "{{row.title}}",
    {% endfor %}
    -- sum(case when t.measure = 'in' and t.inscompany_id = 1 then cnt end) as "Согласие",

    sum(case when t.measure = 'in' and t.inscompany_id is null then cnt end) as "Итого вход",
    -- Выход
    sum(case when t.measure = 'out_repair' then cnt end) as "Выход (ремонт)",
    sum(case when t.measure = 'out_wait_paymenet' then cnt end) as "Выход (ожидание оплаты)",


    sum(case when t.measure = 'out_archive_payment' then cnt end) as "Выход (архив выплата)",
    sum(case when t.measure = 'out_archive_reject' then cnt end) as "Выход (архив отказ)",
    --
    min(d_min) as d_min,
    max(d_max) as d_max,
    max(row_attributes) as row_attributes
from combined_cumul t
group by t.d_create
order by max(d_max), max(period_len)

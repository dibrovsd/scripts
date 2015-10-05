with params as (
    select
        [[env.curator]]::integer as curator,
        [[env.inscompany]]::integer as inscompany,
        [[env.period.0]]::date as d_start,
        [[env.period.1]]::date as d_end

        -- 0 as curator,
        -- 0 as inscompany,
        -- current_date - 60 as d_start,
        -- current_date as d_end
),

base as (
    select
        d.{{env.group_by}} as group_field,
        d.{{env.group_by}}_id as group_field_id,
        -- d.city as group_field,
        -- d.city_id as group_field_id,
        d.id,
        --
        d.d_create,
        d.repair_date_real,
        d.pay_date,
        d.pay_sum
    from reports.v_document d
    cross join params
    where 1 = 1
        and (params.inscompany = 0 or d.inscompany_id = params.inscompany)
        and (params.curator = 0 or d.curator_id = params.curator)
        and d.{{env.group_by}}_id is not null
),

base_event as (
    select d.group_field_id,
        e.state_to_id
    from base d
    cross join params
    inner join reports.rep_1_last_events e on d.id = e.document_id
    where e.d_create between params.d_start and params.d_end
),

base_skeleton as (
    select distinct
        group_field,
        group_field_id
    from base
    order by group_field
),

incoming as (
    select
        -- count(case
        --         when d.d_create between to_date('2015-09-01', 'yyyy-mm-dd')
        --                             and to_date('2015-10-01', 'yyyy-mm-dd') - interval '1 second'
        --         then 1
        -- end) as "2015-07",
        {% for row in datasets.periods.data %}
        count(case
                when d.d_create between to_date('{{row.d_start}}', 'yyyy-mm-dd')
                                    and to_date('{{row.d_end}}', 'yyyy-mm-dd') - interval '1 second'
                then 1
        end) as "{{row.title}}",
        {% endfor %}
        d.group_field_id
    from base d
    cross join params
    where d.d_create between params.d_start and params.d_end
    group by d.group_field_id
),

out_repair as (
    select d.group_field_id,
           count(1) as cnt
    from base d
    cross join params
    where d.repair_date_real between params.d_start and params.d_end
    group by d.group_field_id
),

out_wait_paymenet as (
    select e.group_field_id,
           count(1) as cnt
    from base_event e
    where e.state_to_id = 11 -- Ожидание оплаты
    group by e.group_field_id
),

out_pay as (
    select d.group_field_id,
           count(1) as cnt,
           sum(d.pay_sum) as pay_sum
    from base d
    cross join params
    where d.pay_date between params.d_start and params.d_end
    group by d.group_field_id
),

out_archive as (
    select e.group_field_id,
           count(1) as cnt
    from base_event e
    where e.state_to_id = 12 -- Архив
    group by e.group_field_id
),

data as (
    select s.group_field,
        incoming.*,
        out_repair.cnt as out_repair_cnt,
        out_wp.cnt as out_wp_cnt,
        out_pay.cnt as out_pay_cnt,
        out_pay.pay_sum as out_pay_sum,
        out_archive.cnt as out_archive_cnt
    from base_skeleton s
    left join incoming on incoming.group_field_id = s.group_field_id
    left join out_repair on out_repair.group_field_id = s.group_field_id
    left join out_wait_paymenet out_wp on out_wp.group_field_id = s.group_field_id
    left join out_pay on out_pay.group_field_id = s.group_field_id
    left join out_archive on out_archive.group_field_id = s.group_field_id
)

select
    t.group_field,
    t.group_field_id,
    {% for row in datasets.periods.data %}
    t."{{row.title}}",
    {% endfor %}
    t.out_repair_cnt,
    t.out_wp_cnt,
    t.out_pay_cnt,
    t.out_pay_sum,
    t.out_archive_cnt
from data t

union all

select
    'Итого' as group_field,
    null as group_field_id,
    {% for row in datasets.periods.data %}
    sum(t."{{row.title}}") as "{{row.title}}",
    {% endfor %}
    sum(t.out_repair_cnt) as out_repair_cnt,
    sum(t.out_wp_cnt) as out_wp_cnt,
    sum(t.out_pay_cnt) as out_pay_cnt,
    sum(t.out_pay_sum) as out_pay_sum,
    sum(t.out_archive_cnt) as out_archive_cnt
from data t

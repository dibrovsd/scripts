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

        {% if 'customer_service' in user_params.roles %}
           and d.curator_id = {{user.id}}

        {% elif 'stoa' in user_params.roles %}
           and d.stoa_id in ({{user.stations_ids|join:","}})

        {% endif %}
),

base_event as (
    select d.group_field_id,
        e.state_to_id
    from base d
    cross join params
    inner join reports.rep_1_last_events e on d.id = e.document_id
    where e.d_create between params.d_start and params.d_end
),

-- На этот скелет будем нанизывать группировки по разным
base_skeleton as (
    {% if env.group_by == 'city' %}
        select id, title from base_city

    {% elif env.group_by == 'stoa' %}
        select
            s.id,
            c.title ||' > '|| sc.title || ' > ' || s.title as title
        from base_stoa s
        join base_stoacompany sc on sc.id = s.company_id
        join base_city c on c.id = s.city_id

    {% elif env.group_by == 'stoa_company' %}
        select
            comp.id,
            comp.title || ' ('|| stoa.city_title ||')' as title
        from base_stoacompany comp
        left join (
            select
                stoa.company_id,
                max(city.title) as city_title
            from base_stoa stoa
            inner join base_city city on city.id = stoa.city_id
            group by stoa.company_id
        ) stoa on stoa.company_id = comp.id
        order by title

    {% endif %}

    -- select
    --     group_field,
    --     group_field_id
    -- from base
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
    select s.id as group_id,
        s.title as group_title,
        incoming.*,
        out_repair.cnt as out_repair_cnt,
        out_wp.cnt as out_wp_cnt,
        out_pay.cnt as out_pay_cnt,
        out_pay.pay_sum as out_pay_sum,
        out_archive.cnt as out_archive_cnt
    from base_skeleton s
    left join incoming on incoming.group_field_id = s.id
    left join out_repair on out_repair.group_field_id = s.id
    left join out_wait_paymenet out_wp on out_wp.group_field_id = s.id
    left join out_pay on out_pay.group_field_id = s.id
    left join out_archive on out_archive.group_field_id = s.id
)

select
    t.group_id,
    t.group_title,
    {% for row in datasets.periods.data %}
    t."{{row.title}}",
    {% endfor %}
    t.out_repair_cnt,
    t.out_wp_cnt,
    t.out_pay_cnt,
    t.out_pay_sum,
    t.out_archive_cnt,
    1 as group_order
from data t

union all

select
    0 as group_id,
    'Итого' as group_title,
    {% for row in datasets.periods.data %}
    sum(t."{{row.title}}") as "{{row.title}}",
    {% endfor %}
    sum(t.out_repair_cnt) as out_repair_cnt,
    sum(t.out_wp_cnt) as out_wp_cnt,
    sum(t.out_pay_cnt) as out_pay_cnt,
    sum(t.out_pay_sum) as out_pay_sum,
    sum(t.out_archive_cnt) as out_archive_cnt,
    2 as group_order
from data t

order by group_order, "ИтогоВход" desc nulls last

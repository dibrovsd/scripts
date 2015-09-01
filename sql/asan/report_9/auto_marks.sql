with params as (
    select
           {% if not env.period_type or env.period_type == 'month' %}
                date_trunc('month', current_date) as d_start,
                current_date + interval '1 day - 1 second' as d_end

           {% else %}
               [[env.period.0]] as d_start,
               [[env.period.1]] as d_end

           {% endif %}


           -- to_date('01.01.2015', 'dd.mm.yyyy') as d_start,
           -- to_date('01.05.2015', 'dd.mm.yyyy') - 1 as d_end
),

sales as (
    -- ОСАГО
    select 'ОСАГО' as product, t.ins_person_birthday, t.auto_createyear, t.auto_mark_id, t.ins_person_pin, t.ins_person_gender, t.seller_territory_id
    from reports.base_osago t
    cross join params
    where t.d_issue between params.d_start and params.d_end

    -- Уверенный водитель
    union all
    select 'Уверенный водитель' as product, t.ins_person_birthday, t.auto_createyear, t.auto_mark_id, t.ins_person_pin, t.ins_person_gender, t.seller_territory_id
    from reports.base_confident_driver t
    cross join params
    where t.d_issue between params.d_start and params.d_end

    -- Просто КАСКО
    union all
    select 'Просто КАСКО' as product, t.ins_person_birthday, t.auto_createyear, t.auto_mark_id, t.ins_person_pin, t.ins_person_gender, t.seller_territory_id
    from reports.base_simple_kasko t
    cross join params
    where t.d_issue between params.d_start and params.d_end

),

sales1 as (
    select t.auto_mark_id,
        count(1) as cnt
    from sales t
    where t.auto_createyear is not null
    {% if env.seller_territory == 'call_centre' %}
        and seller_territory_id = 9
    {% elif env.seller_territory == 'asan' %}
        and seller_territory_id != 9
    {% endif %}

    group by t.auto_mark_id
)

select
    auto_m.id,
    auto_m.title
from sales1
join docflow_automark auto_m on auto_m.id = sales1.auto_mark_id
order by sales1.cnt desc
limit 10

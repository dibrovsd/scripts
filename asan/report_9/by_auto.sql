with params as (
    select
           {% if not env.period_type or env.period_type == 'month' %}
                date_trunc('month', current_date) as d_start,
                current_date + interval '1 day - 1 second' as d_end

           {% else %}
               [[env.period.0]] as d_start,
               [[env.period.1]] as d_end

           {% endif %}

           --to_date('01.01.2015', 'dd.mm.yyyy') as d_start,
           --to_date('01.05.2015', 'dd.mm.yyyy') - 1 as d_end
),

-- Сводим продукты
sales as (
    -- ОСАГО
    select 'ОСАГО' as product, t.ins_person_birthday, t.auto_createyear, t.auto_mark_id, t.ins_person_pin, t.ins_person_gender, t.channel_root_id
    from reports.base_osago t
    cross join params
    where t.d_issue between params.d_start and params.d_end

    -- Уверенный водитель
    union all
    select 'Уверенный водитель' as product, t.ins_person_birthday, t.auto_createyear, t.auto_mark_id, t.ins_person_pin, t.ins_person_gender, t.channel_root_id
    from reports.base_confident_driver t
    cross join params
    where t.d_issue between params.d_start and params.d_end

    -- Просто КАСКО
    union all
    select 'Просто КАСКО' as product, t.ins_person_birthday, t.auto_createyear, t.auto_mark_id, t.ins_person_pin, t.ins_person_gender, t.channel_root_id
    from reports.base_simple_kasko t
    cross join params
    where t.d_issue between params.d_start and params.d_end

),

-- Расчет измерений и ограничение списка по нужным маркам
sales1 as (
    select t.auto_mark_id,
        case
            when t.auto_createyear > extract(year from current_date) - 3 then '< 3'
            when t.auto_createyear > extract(year from current_date) - 5 then '3 - 5'
            when t.auto_createyear > extract(year from current_date) - 10 then '5 - 10'
            else '> 10'
        end as auto_age
    from sales t
    inner join (
        {{datasets.auto_marks.sql}}
    ) selected_marks on selected_marks.id = t.auto_mark_id
    where t.auto_createyear is not null

    {% if env.product != '' %}
        and t.product = [[env.product]]
    {% endif %}

    {% if env.channel %}
        and t.channel_root_id = [[env.channel]]::integer
    {% endif %}

    {% if 'call_center' in user_params.territory_only %}
        and t.channel_root_id = 9
    {% elif 'asan' in user_params.territory_only %}
        and t.channel_root_id = 7
    {% endif %}

),

-- Группируем строки по измерениям
sales2 as (
    select
        auto_age,
        auto_mark_id,
        count(1) as cnt
    from sales1
    group by auto_mark_id, auto_age
)

select
    auto_age as "Возраст ТС",
    {% for mark in datasets.auto_marks.data %}
        round(sum(case when auto_mark_id = {{mark.id}} then sales2.cnt end) /
        sum(sum(case when auto_mark_id = {{mark.id}} then sales2.cnt end)) over()
        * 100) ||'%' as "{{mark.title}}",
    {% endfor %}
    null as dummy
from sales2
group by auto_age

union all

select
    'Итого' as auto_age,
    {% for mark in datasets.auto_marks.data %}
        round(sum(case when auto_mark_id = {{mark.id}} then sales2.cnt end) /
        sum(sales2.cnt)
        * 100) ||'%' as "{{mark.title}}",
    {% endfor %}
    null as dummy
from sales2

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
    select 'ОСАГО' as product, t.ins_person_birthday, t.auto_createyear, t.auto_mark_id,
            t.ins_person_pin, t.ins_person_gender, t.channel_root_id, t.channel_sub_id, t.channel_territory_id
    from reports.base_osago t
    cross join params
    where t.d_issue between params.d_start and params.d_end

    -- Недвижимость
    union all
    select 'Недвижимость' as product, t.ins_person_birthday, null as auto_createyear, null as auto_mark_id,
            t.ins_person_pin, t.ins_person_gender, t.channel_root_id, t.channel_sub_id, t.channel_territory_id
    from reports.base_realty t
    cross join params
    where t.d_issue between params.d_start and params.d_end

    -- ВЗР
    union all
    select 'ВЗР' as product, t.ins_person_birthday, null as auto_createyear, null as auto_mark_id,
            t.ins_person_pin, t.ins_person_gender, t.channel_root_id, t.channel_sub_id, t.channel_territory_id
    from reports.base_travel t
    cross join params
    where t.d_issue between params.d_start and params.d_end

    -- Уверенный водитель
    union all
    select 'Уверенный водитель' as product, t.ins_person_birthday, t.auto_createyear, t.auto_mark_id,
            t.ins_person_pin, t.ins_person_gender, t.channel_root_id, t.channel_sub_id, t.channel_territory_id
    from reports.base_confident_driver t
    cross join params
    where t.d_issue between params.d_start and params.d_end

    -- Просто КАСКО
    union all
    select 'Просто КАСКО' as product, t.ins_person_birthday, t.auto_createyear, t.auto_mark_id,
            t.ins_person_pin, t.ins_person_gender, t.channel_root_id, t.channel_sub_id, t.channel_territory_id
    from reports.base_simple_kasko t
    cross join params
    where t.d_issue between params.d_start and params.d_end

    -- Пятерочка
    union all
    select 'Пятерочка' as product, t.ins_person_birthday, t.auto_createyear, t.auto_mark_id,
            t.ins_person_pin, t.ins_person_gender, t.channel_root_id, t.channel_sub_id, t.channel_territory_id
    from reports.base_raider_five t
    cross join params
    where t.d_issue between params.d_start and params.d_end

    -- ОСАГО+
    union all
    select 'ОСАГО+' as product, t.ins_person_birthday, t.auto_createyear, t.auto_mark_id,
            t.ins_person_pin, t.ins_person_gender, t.channel_root_id, t.channel_sub_id, t.channel_territory_id
    from reports.base_raider_osago_plus t
    cross join params
    where t.d_issue between params.d_start and params.d_end

    -- Супер КАСКО
    union all
    select 'Супер КАСКО' as product, t.ins_person_birthday, t.auto_createyear, t.auto_mark_id,
            t.ins_person_pin, t.ins_person_gender, t.channel_root_id, t.channel_sub_id, t.channel_territory_id
    from reports.base_raider_super_kasko t
    cross join params
    where t.d_issue between params.d_start and params.d_end
),

by_age as (
    select t.*,
        case
            when t.ins_person_birthday > current_date - interval '20 year' then '< 20'
            when t.ins_person_birthday > current_date - interval '30 year' then '20 - 30'
            when t.ins_person_birthday > current_date - interval '40 year' then '30 - 40'
            when t.ins_person_birthday > current_date - interval '50 year' then '40 - 50'
            else '> 50'
        end as insurer_age
    from sales t
    where t.ins_person_birthday is not null

    {% if env.product != '' %}
        and t.product = [[env.product]]
    {% endif %}

    {% if env.channel %}
        and [[env.channel]]::integer in (t.channel_root_id, t.channel_sub_id, t.channel_territory_id)
    {% endif %}

    {% if 'call_center' in user_params.territory_only %}
        and t.channel_root_id = 9
    {% elif 'asan' in user_params.territory_only %}
        and t.channel_root_id = 7
    {% endif %}

),

by_age1 as (
    select
        ins_person_gender,
        count(distinct t.ins_person_pin) as cnt_all,
        count(distinct case when insurer_age = '< 20' then t.ins_person_pin end) as range_20,
        count(distinct case when insurer_age = '20 - 30' then t.ins_person_pin end) as range_20_30,
        count(distinct case when insurer_age = '30 - 40' then t.ins_person_pin end) as range_30_40,
        count(distinct case when insurer_age = '40 - 50' then t.ins_person_pin end) as range_40_50,
        count(distinct case when insurer_age = '> 50' then t.ins_person_pin end) as range_50
    from by_age t
    group by ins_person_gender
),

by_age2 as (
    select
        sum(range_20) / sum(cnt_all) * 100 as range_20,
        sum(range_20_30) / sum(cnt_all) * 100 as range_20_30,
        sum(range_30_40) / sum(cnt_all) * 100 as range_30_40,
        sum(range_40_50) / sum(cnt_all) * 100 as range_40_50,
        sum(range_50) / sum(cnt_all) * 100 as range_50
    from by_age1
)

select
    t.ins_person_gender,
    round(t.range_20) ||'%' as range_20,
    round(t.range_20_30) ||'%' as range_20_30,
    round(t.range_30_40) ||'%' as range_30_40,
    round(t.range_40_50) ||'%' as range_40_50,
    round(t.range_50) ||'%' as range_50
from (
    select
        null as ins_person_gender,
        range_20,
        range_20_30,
        range_30_40,
        range_40_50,
        range_50
    from by_age2
    union all
    select
        case
            when ins_person_gender = 1 then 'М'
            when ins_person_gender = 2 then 'Ж'
        end as ins_person_gender,
        case when sum(range_20) over() > 0 then range_20 / sum(range_20) over() * 100 end as range_20,
        case when sum(range_20_30) over() > 0 then range_20_30 / sum(range_20_30) over() * 100 end as range_20_30,
        case when sum(range_30_40) over() > 0 then range_30_40 / sum(range_30_40) over() * 100 end as range_30_40,
        case when sum(range_40_50) over() > 0 then range_40_50 / sum(range_40_50) over() * 100 end as range_40_50,
        case when sum(range_50) over() > 0 then range_50 / sum(range_50) over() * 100 end as range_50
    from by_age1
) t

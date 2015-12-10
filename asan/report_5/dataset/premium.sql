-- Параметры
with params as (
    select
        {% if not env.period_type or env.period_type == 'month' %}
            date_trunc('month', current_date) as d_start,
            current_date + interval '1 day - 1 second' as d_end,

        {% else %}
           [[env.period.0]] as d_start,
           [[env.period.1]] as d_end,

        {% endif %}
        [[env.group_by]]::varchar as trunc_by,
        interval '1 {{env.group_by}}' as interv,
        [[env.inscompany]]::int as inscompany

        -- to_date('08.05.2015', 'dd.mm.yyyy') as d_start,
        -- to_date('08.05.2015', 'dd.mm.yyyy') + interval '1 day - 1 second' as d_end,
        -- 'week'::varchar as trunc_by,
        -- interval '1 week' as interv,
        -- 0 as inscompany,
        -- null::integer as territory_id
),

gr as (
    select
        date_trunc(params.trunc_by, s.d_issue)::date as dt,
        --
        count(1) as all_cnt,
        coalesce(sum(s.s_premium), 0) as all_premium,
        --
        count(case when s.product = 'ОСАГО' then 1 end) as osago_cnt,
        coalesce(sum(case when s.product = 'ОСАГО' then s.s_premium end), 0) as osago_premium,
        --
        count(case when s.product = 'Недвижимость' then 1 end) as realty_cnt,
        coalesce(sum(case when s.product = 'Недвижимость' then s.s_premium end), 0) as realty_premium,
        --
        count(case when s.product = 'ВЗР' then 1 end) as travel_cnt,
        coalesce(sum(case when s.product = 'ВЗР' then s.s_premium end), 0) as travel_premium,
        --
        count(case when s.product = 'Уверенный водитель' then 1 end) as confident_driver_cnt,
        coalesce(sum(case when s.product = 'Уверенный водитель' then s.s_premium end), 0) as confident_driver_premium,
        --
        count(case when s.product = 'Просто КАСКО' then 1 end) as simple_kasko_cnt,
        coalesce(sum(case when s.product = 'Просто КАСКО' then s.s_premium end), 0) as simple_kasko_premium,
        --
        count(case when s.product = 'Пятерочка' then 1 end) as raider_five_cnt,
        coalesce(sum(case when s.product = 'Пятерочка' then s.s_premium end), 0) as raider_five_premium,
        --
        count(case when s.product = 'ОСАГО+' then 1 end) as raider_osago_plus_cnt,
        coalesce(sum(case when s.product = 'ОСАГО+' then s.s_premium end), 0) as raider_osago_plus_premium,
        --
        count(case when s.product = 'Супер КАСКО' then 1 end) as raider_super_kasko_cnt,
        coalesce(sum(case when s.product = 'Супер КАСКО' then s.s_premium end), 0) as raider_super_kasko_premium
        --
    from reports.base_sales s
    cross join params
    where (params.inscompany = 0 or s.inscompany_id = params.inscompany)
        and s.d_issue between params.d_start and params.d_end

        {% if env.channel %}
            and [[env.channel]]::integer in (s.channel_root_id, s.channel_sub_id, s.channel_territory_id)
        {% endif %}

        {% if 'call_center' in user_params.territory_only %}
            and s.channel_root_id = 9
        {% elif 'asan' in user_params.territory_only %}
            and s.channel_root_id = 7
        {% endif %}

    group by date_trunc(params.trunc_by, s.d_issue)::date
)

select
    dt,
    --
    sum(all_premium) over(order by dt) as premium_all_cum,
    all_premium as premium_all,
    --
    osago_premium,
    realty_premium,
    travel_premium,
    confident_driver_premium,
    simple_kasko_premium,
    raider_five_premium,
    raider_osago_plus_premium,
    raider_super_kasko_premium,
    --
    all_cnt as cnt_all,
    osago_cnt,
    realty_cnt,
    travel_cnt,
    confident_driver_cnt,
    simple_kasko_cnt,
    raider_five_cnt,
    raider_osago_plus_cnt,
    raider_super_kasko_cnt,
    '' as row_style
from gr

union all

select
    null as dt,
    null as premium_all_cum,
    --
    sum(all_premium),
    sum(osago_premium),
    sum(realty_premium),
    sum(travel_premium),
    sum(confident_driver_premium),
    sum(simple_kasko_premium),
    sum(raider_five_premium),
    sum(raider_osago_plus_premium),
    sum(raider_super_kasko_premium),
    --
    sum(osago_cnt + realty_cnt + travel_cnt + confident_driver_cnt
    + simple_kasko_cnt + raider_five_cnt + raider_osago_plus_cnt
    + raider_super_kasko_cnt),
    sum(osago_cnt),
    sum(realty_cnt),
    sum(travel_cnt),
    sum(confident_driver_cnt),
    sum(simple_kasko_cnt),
    sum(raider_five_cnt),
    sum(raider_osago_plus_cnt),
    sum(raider_super_kasko_cnt),
    'font-weight: bold; background-color: #f5f5f5;' as row_style
from gr
order by dt nulls last

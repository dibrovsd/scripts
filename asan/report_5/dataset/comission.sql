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
        [[env.inscompany]]::int as inscompany,
        {% if env.territory_id %}[[env.territory_id]]{% else %}null{% endif %}::int as territory_id

        -- to_date('01.01.2015', 'dd.mm.yyyy') as d_start,
        -- current_date as d_end,
        -- 'week'::varchar as trunc_by,
        -- interval '1 week' as interv,
        -- 0 as inscompany,
        -- null::int as territory_id

),

sales as (
    select date_trunc(params.trunc_by, s.d_issue)::date as dt,
        coalesce(sum(s.s_comission), 0) as comission_all,
        coalesce(sum(case when s.product = 'ОСАГО' then s.s_comission end), 0) as osago_comission,
        coalesce(sum(case when s.product = 'Недвижимость' then s.s_comission end), 0) as realty_comission,
        coalesce(sum(case when s.product = 'ВЗР' then s.s_comission end), 0) as travel_comission,
        coalesce(sum(case when s.product = 'Уверенный водитель' then s.s_comission end), 0) as confident_driver_comission,
        coalesce(sum(case when s.product = 'Просто КАСКО' then s.s_comission end), 0) as simple_kasko_comission,
        coalesce(sum(case when s.product = 'Пятерочка' then s.s_comission end), 0) as raider_five_comission,
        coalesce(sum(case when s.product = 'ОСАГО+' then s.s_comission end), 0) as raider_osago_plus_comission,
        coalesce(sum(case when s.product = 'Супер КАСКО' then s.s_comission end), 0) as raider_super_kasko_comission
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
    s.dt,
    sum(s.comission_all) over(order by s.dt) as comission_all_cum,
    s.comission_all,
    s.osago_comission,
    s.realty_comission,
    s.travel_comission,
    s.confident_driver_comission,
    s.simple_kasko_comission,
    s.raider_five_comission,
    s.raider_osago_plus_comission,
    s.raider_super_kasko_comission,
    '' as row_style
from sales s

union all

select
    null as dt,
    null as comission_all_cum,
    sum(s.comission_all) as comission_all,
    sum(s.osago_comission) as osago_comission,
    sum(s.realty_comission) as realty_comission,
    sum(s.travel_comission) as travel_comission,
    sum(s.confident_driver_comission) as confident_driver_comission,
    sum(s.simple_kasko_comission) as simple_kasko_comission,
    sum(s.raider_five_comission) as raider_five_comission,
    sum(s.raider_osago_plus_comission) as raider_osago_plus_comission,
    sum(s.raider_super_kasko_comission) as raider_super_kasko_comission,
    'font-weight: bold; background-color: #f5f5f5;' as row_style
from sales s
order by dt nulls last

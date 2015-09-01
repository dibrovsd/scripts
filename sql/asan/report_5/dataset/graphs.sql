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
        -- current_date + interval '1 day - 1 second' as d_end,
        -- 'week'::varchar as trunc_by,
        -- interval '1 week' as interv,
        -- 0 as inscompany,
        -- null::integer as territory_id
)

select
    date_trunc(params.trunc_by, s.d_issue)::date as dt,
    --
    sum(case when s.product = 'ОСАГО' then 1 end) as osago_cnt,
    sum(case when s.product = 'ОСАГО' then s.s_premium end) as osago_premium,
    --
    sum(case when s.product = 'Недвижимость' then 1 end) as realty_cnt,
    sum(case when s.product = 'Недвижимость' then s.s_premium end) as realty_premium,
    --
    sum(case when s.product = 'ВЗР' then 1 end) as travel_cnt,
    sum(case when s.product = 'ВЗР' then s.s_premium end) as travel_premium,
    --
    sum(case when s.product = 'Уверенный водитель' then 1 end) as confident_driver_cnt,
    sum(case when s.product = 'Уверенный водитель' then s.s_premium end) as confident_driver_premium,
    --
    sum(case when s.product = 'Просто КАСКО' then 1 end) as simple_kasko_cnt,
    sum(case when s.product = 'Просто КАСКО' then s.s_premium end) as simple_kasko_premium,
    --
    sum(case when s.product = 'Пятерочка' then 1 end) as raider_five_cnt,
    sum(case when s.product = 'Пятерочка' then s.s_premium end) as raider_five_premium,
    --
    sum(case when s.product = 'ОСАГО+' then 1 end) as raider_osago_plus_cnt,
    sum(case when s.product = 'ОСАГО+' then s.s_premium end) as raider_osago_plus_premium,
    --
    sum(case when s.product = 'Супер КАСКО' then 1 end) as raider_super_kasko_cnt,
    sum(case when s.product = 'Супер КАСКО' then s.s_premium end) as raider_super_kasko_premium
    --
from reports.base_sales s
cross join params
where (params.inscompany = 0 or s.inscompany_id = params.inscompany)
    and s.d_issue between params.d_start and params.d_end
    {% if env.seller_territory == 'call_centre' %}
        and s.seller_territory_id = 9
    {% elif env.seller_territory == 'asan' %}
        and s.seller_territory_id != 9
        and (params.territory_id is null or s.seller_territory_id = params.territory_id)
    {% endif %}
group by date_trunc(params.trunc_by, s.d_issue)::date
order by dt

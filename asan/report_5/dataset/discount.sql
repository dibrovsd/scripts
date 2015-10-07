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

        -- to_date('01.08.2015', 'dd.mm.yyyy') as d_start,
        -- current_date as d_end,
        -- 'day'::varchar as trunc_by,
        -- interval '1 day' as interv,
        -- 0 as inscompany,
        -- null::integer as territory_id

),

sales_gr as (
    select
        date_trunc(params.trunc_by, s.d_issue)::date as dt,
        s.product,
        count(1) as cnt_all,
        sum(s.s_discount) as s_discount,
        count(case when s.s_discount > 0 then 1 end) as cnt_discount
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
    group by date_trunc(params.trunc_by, s.d_issue)::date, s.product
),

by_products as (
    select
        s.dt,
        --
        sum(s.s_discount) as all_discount_sum,
        sum(s.cnt_discount) as all_discount_cnt,
        sum(s.cnt_all) as all_cnt,
        --
        sum(case when s.product = 'ОСАГО' then s.s_discount end) as osago_sum,
        sum(case when s.product = 'ОСАГО' then s.cnt_discount end) as osago_cnt,
        sum(case when s.product = 'ОСАГО' then s.cnt_all end) as osago_cnt_all,
        --
        sum(case when s.product = 'Недвижимость' then s.s_discount end) as realty_sum,
        sum(case when s.product = 'Недвижимость' then s.cnt_discount end) as realty_cnt,
        sum(case when s.product = 'Недвижимость' then s.cnt_all end) as realty_cnt_all,
        --
        sum(case when s.product = 'ВЗР' then s.s_discount end) as travel_sum,
        sum(case when s.product = 'ВЗР' then s.cnt_discount end) as travel_cnt,
        sum(case when s.product = 'ВЗР' then s.cnt_all end) as travel_cnt_all,
        --
        sum(case when s.product = 'Уверенный водитель' then s.s_discount end) as confident_driver_sum,
        sum(case when s.product = 'Уверенный водитель' then s.cnt_discount end) as confident_driver_cnt,
        sum(case when s.product = 'Уверенный водитель' then s.cnt_all end) as confident_driver_cnt_all,
        --
        sum(case when s.product = 'Просто КАСКО' then s.s_discount end) as simple_kasko_sum,
        sum(case when s.product = 'Просто КАСКО' then s.cnt_discount end) as simple_kasko_cnt,
        sum(case when s.product = 'Просто КАСКО' then s.cnt_all end) as simple_kasko_cnt_all,
        --
        sum(case when s.product = 'Пятерочка' then s.s_discount end) as raider_five_sum,
        sum(case when s.product = 'Пятерочка' then s.cnt_discount end) as raider_five_cnt,
        sum(case when s.product = 'Пятерочка' then s.cnt_all end) as raider_five_cnt_all,
        --
        sum(case when s.product = 'ОСАГО+' then s.s_discount end) as raider_osago_plus_sum,
        sum(case when s.product = 'ОСАГО+' then s.cnt_discount end) as raider_osago_plus_cnt,
        sum(case when s.product = 'ОСАГО+' then s.cnt_all end) as raider_osago_plus_cnt_all,
        --
        sum(case when s.product = 'Супер КАСКО' then s.s_discount end) as raider_super_kasko_sum,
        sum(case when s.product = 'Супер КАСКО' then s.cnt_discount end) as raider_super_kasko_cnt,
        sum(case when s.product = 'Супер КАСКО' then s.cnt_all end) as raider_super_kasko_cnt_all
    from sales_gr s
    group by s.dt
),

cumul as (
    select
        s.dt,
        sum(s.all_discount_sum) over(order by dt) as discount_all_cum,
        s.all_discount_sum as discount_all,

        -- Суммы скидок
        s.osago_sum as osago_s_discount,
        s.realty_sum as realty_s_discount,
        s.travel_sum as travel_s_discount,
        s.confident_driver_sum as confident_driver_s_discount,
        s.simple_kasko_sum as simple_kasko_s_discount,
        s.raider_five_sum as raider_five_s_discount,
        s.raider_osago_plus_sum as raider_osago_plus_s_discount,
        s.raider_super_kasko_sum as raider_super_kasko_s_discount,

        -- Доля
        f_division(s.all_discount_cnt, s.all_cnt) * 100 as ratio_all,
        f_division(s.osago_cnt, s.osago_cnt_all) * 100 as osago_ratio,
        f_division(s.realty_cnt, s.realty_cnt_all) * 100 as realty_ratio,
        f_division(s.travel_cnt, s.travel_cnt_all) * 100 as travel_ratio,
        f_division(s.confident_driver_cnt, s.confident_driver_cnt_all) * 100 as confident_driver_ratio,
        f_division(s.simple_kasko_cnt, s.simple_kasko_cnt_all) * 100 as simple_kasko_ratio,
        f_division(s.raider_five_cnt, s.raider_five_cnt_all) * 100 as raider_five_ratio,
        f_division(s.raider_osago_plus_cnt, s.raider_osago_plus_cnt_all) * 100 as raider_osago_plus_ratio,
        f_division(s.raider_super_kasko_cnt, s.raider_super_kasko_cnt_all) * 100 as raider_super_kasko_ratio,

        -- Средняя
        f_division(s.osago_sum, s.osago_cnt) as osago_avg,
        f_division(s.realty_sum, s.realty_cnt) as realty_avg,
        f_division(s.travel_sum, s.travel_cnt) as travel_avg,
        f_division(s.confident_driver_sum, s.confident_driver_cnt) as confident_driver_avg,
        f_division(s.simple_kasko_sum, s.simple_kasko_cnt) as simple_kasko_avg,
        f_division(s.raider_five_sum, s.raider_five_cnt) as raider_five_avg,
        f_division(s.raider_osago_plus_sum, s.raider_osago_plus_cnt) as raider_osago_plus_avg,
        f_division(s.raider_super_kasko_sum, s.raider_super_kasko_cnt) as raider_super_kasko_avg,
        --
        '' as row_style

    from by_products s

    union all

    select
        null as dt,
        null as discount_all_cum,
        sum(s.all_discount_sum),

        -- Суммы скидок
        sum(s.osago_sum),
        sum(s.realty_sum),
        sum(s.travel_sum),
        sum(s.confident_driver_sum),
        sum(s.simple_kasko_sum),
        sum(s.raider_five_sum),
        sum(s.raider_osago_plus_sum),
        sum(s.raider_super_kasko_sum),

        -- Доля
        f_division(sum(s.all_discount_cnt), sum(s.all_cnt)) * 100 as ratio_all,
        f_division(sum(s.osago_cnt), sum(s.osago_cnt_all)) * 100 as osago_ratio,
        f_division(sum(s.realty_cnt), sum(s.realty_cnt_all)) * 100 as realty_ratio,
        f_division(sum(s.travel_cnt), sum(s.travel_cnt_all)) * 100 as travel_ratio,
        f_division(sum(s.confident_driver_cnt), sum(s.confident_driver_cnt_all)) * 100 as confident_driver_ratio,
        f_division(sum(s.simple_kasko_cnt), sum(s.simple_kasko_cnt_all)) * 100 as simple_kasko_ratio,
        f_division(sum(s.raider_five_cnt), sum(s.raider_five_cnt_all)) * 100 as raider_five_ratio,
        f_division(sum(s.raider_osago_plus_cnt), sum(s.raider_osago_plus_cnt_all)) * 100 as raider_osago_plus_ratio,
        f_division(sum(s.raider_super_kasko_cnt), sum(s.raider_super_kasko_cnt_all)) * 100 as raider_super_kasko_ratio,

        -- Средняя
        f_division(sum(s.osago_sum), sum(s.osago_cnt)) as osago_avg,
        f_division(sum(s.realty_sum), sum(s.realty_cnt)) as realty_avg,
        f_division(sum(s.travel_sum), sum(s.travel_cnt)) as travel_avg,
        f_division(sum(s.confident_driver_sum), sum(s.confident_driver_cnt)) as confident_driver_avg,
        f_division(sum(s.simple_kasko_sum), sum(s.simple_kasko_cnt)) as simple_kasko_avg,
        f_division(sum(s.raider_five_sum), sum(s.raider_five_cnt)) as raider_five_avg,
        f_division(sum(s.raider_osago_plus_sum), sum(s.raider_osago_plus_cnt)) as raider_osago_plus_avg,
        f_division(sum(s.raider_super_kasko_sum), sum(s.raider_super_kasko_cnt)) as raider_super_kasko_avg,
        --
        'font-weight: bold; background-color: #f5f5f5;' as row_style
    from by_products s
)

-- Правим нолики
select
    dt,
    discount_all_cum,
    coalesce(discount_all, 0) as discount_all,
    coalesce(osago_s_discount, 0) as osago_s_discount,
    coalesce(realty_s_discount, 0) as realty_s_discount,
    coalesce(travel_s_discount, 0) as travel_s_discount,
    coalesce(confident_driver_s_discount, 0) as confident_driver_s_discount,
    coalesce(simple_kasko_s_discount, 0) as simple_kasko_s_discount,
    coalesce(raider_five_s_discount, 0) as raider_five_s_discount,
    coalesce(raider_osago_plus_s_discount, 0) as raider_osago_plus_s_discount,
    coalesce(raider_super_kasko_s_discount, 0) as raider_super_kasko_s_discount,
    coalesce(ratio_all, 0) as ratio_all,
    coalesce(osago_ratio, 0) as osago_ratio,
    coalesce(realty_ratio, 0) as realty_ratio,
    coalesce(travel_ratio, 0) as travel_ratio,
    coalesce(confident_driver_ratio, 0) as confident_driver_ratio,
    coalesce(simple_kasko_ratio, 0) as simple_kasko_ratio,
    coalesce(raider_five_ratio, 0) as raider_five_ratio,
    coalesce(raider_osago_plus_ratio, 0) as raider_osago_plus_ratio,
    coalesce(raider_super_kasko_ratio, 0) as raider_super_kasko_ratio,
    coalesce(osago_avg, 0) as osago_avg,
    coalesce(realty_avg, 0) as realty_avg,
    coalesce(travel_avg, 0) as travel_avg,
    coalesce(confident_driver_avg, 0) as confident_driver_avg,
    coalesce(simple_kasko_avg, 0) as simple_kasko_avg,
    coalesce(raider_five_avg, 0) as raider_five_avg,
    coalesce(raider_osago_plus_avg, 0) as raider_osago_plus_avg,
    coalesce(raider_super_kasko_avg, 0) as raider_super_kasko_avg,
    row_style
from cumul s
order by dt nulls last

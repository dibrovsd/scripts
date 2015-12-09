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

-- План продаж на период
plan as (
    select
        sum(plan_cnt) as plan_cnt
    from (
        select
            dt::date as dt,
            3 as plan_cnt
        from (
            select
                generate_series(params.d_start, params.d_end, interval '1 day') as dt
            from params
        ) t1
        cross join params
        where extract(isodow from t1.dt) != 7
    ) t
),

-- Считаем продажи за период по продавцам
sales as (
    select
        s.seller_id,
        s.product,
        s.s_premium,
        s.s_discount
    from reports.base_sales s
    cross join params
    where s.d_issue between params.d_start and params.d_end

    {% if env.channel %}
        and s.channel_root_id = [[env.channel]]::integer
    {% endif %}

    {% if 'call_center' in user_params.territory_only %}
        and s.channel_root_id = 9
    {% elif 'asan' in user_params.territory_only %}
        and s.channel_root_id = 7
    {% endif %}
),

sales1 as (
    select t.*,
        case
            when product in ('ОСАГО', 'Недвижимость', 'ВЗР') then 1
            else 0
        end as is_policy
    from sales t
),

sales2 as (
    select
        seller_id,
        -- Общие продажи
        count(case when is_policy = 1 then 1 end) as sales_cnt_policy,
        count(case when is_policy = 0 then 1 end) as sales_cnt_raiders,
        -- ОСАГО
        count(case when product = 'ОСАГО' then 1 end) as sales_cnt_osago,
        sum(case when product = 'ОСАГО' then t.s_premium end) as sales_premium_osago,
        sum(case when product = 'ОСАГО' then t.s_discount end) as sales_discount_sum_osago,
        count(case when product = 'ОСАГО' and t.s_discount > 0 then 1 end) as sales_discount_cnt_osago,
        -- Недвижимость
        count(case when product = 'Недвижимость' then 1 end) as sales_cnt_realty,
        sum(case when product = 'Недвижимость' then t.s_premium end) as sales_premium_realty,
        sum(case when product = 'Недвижимость' then t.s_discount end) as sales_discount_sum_realty,
        count(case when product = 'Недвижимость' and t.s_discount > 0 then 1 end) as sales_discount_cnt_realty,
        -- ВЗР
        count(case when product = 'ВЗР' then 1 end) as sales_cnt_travel,
        sum(case when product = 'ВЗР' then t.s_premium end) as sales_premium_travel,
        -- Уверенный водитель
        count(case when product = 'Уверенный водитель' then 1 end) as sales_cnt_confident_driver,
        sum(case when product = 'Уверенный водитель' then t.s_premium end) as sales_premium_confident_driver,
        -- Просто КАСКО
        count(case when product = 'Просто КАСКО' then 1 end) as sales_cnt_simple_kasko,
        sum(case when product = 'Просто КАСКО' then t.s_premium end) as sales_premium_simple_kasko,
        -- Пятерочка
        count(case when product = 'Пятерочка' then 1 end) as sales_cnt_raider_five,
        sum(case when product = 'Пятерочка' then t.s_premium end) as sales_premium_raider_five,
        -- ОСАГО+
        count(case when product = 'ОСАГО+' then 1 end) as sales_cnt_raider_osago_plus,
        sum(case when product = 'ОСАГО+' then t.s_premium end) as sales_premium_raider_osago_plus,
        -- Супер КАСКО
        count(case when product = 'Супер КАСКО' then 1 end) as sales_cnt_raider_super_kasko,
        sum(case when product = 'Супер КАСКО' then t.s_premium end) as sales_premium_raider_super_kasko
    from sales1 t
    group by seller_id
),

res as (
    -- Объединяем
    select
        t1.last_name ||' '|| t1.first_name as seller,
        -- Показатели
        case
            when plan_cnt > 0 then sales_cnt_policy::float / plan_cnt * 100
        end as plan_ratio_policy,
        case
            when sales_cnt_policy > 0 then sales_cnt_raiders::float / sales_cnt_policy * 100
        end as ratio_raiders,
        -- ОСАГО
        sales_premium_osago,
        sales_cnt_osago,
        case
            when sales_cnt_osago > 0 then sales_discount_cnt_osago::float / sales_cnt_osago * 100
        end as sales_discount_ratio_osago,
        sales_discount_sum_osago,
        -- Недвижимость
        sales_premium_realty,
        sales_cnt_realty,
        case
            when sales_cnt_realty > 0 then sales_discount_cnt_realty::float / sales_cnt_realty * 100
        end as sales_discount_ratio_realty,
        sales_discount_sum_realty,
        -- ВЗР
        sales_premium_travel,
        sales_cnt_travel,
        -- Уверенный водитель
        sales_premium_confident_driver,
        sales_cnt_confident_driver,
        -- Просто КАСКО
        sales_premium_simple_kasko,
        sales_cnt_simple_kasko,
        -- Пятерочка
        sales_premium_raider_five,
        sales_cnt_raider_five,
        -- ОСАГО+
        sales_premium_raider_osago_plus,
        sales_cnt_raider_osago_plus,
        -- Супер КАСКО
        sales_premium_raider_super_kasko,
        sales_cnt_raider_super_kasko,
        --
        '' as row_style
    from sales2 t
    cross join plan
    inner join base_user t1 on t1.id = seller_id

    union all

    select
        null as seller,
        -- Показатели
        case
            when sum(plan_cnt) > 0 then sum(sales_cnt_policy::float) / sum(plan_cnt) * 100
        end as plan_ratio_policy,
        case
            when sum(sales_cnt_policy) > 0 then sum(sales_cnt_raiders::float) / sum(sales_cnt_policy) * 100
        end ratio_raiders,
        -- ОСАГО
        sum(sales_premium_osago),
        sum(sales_cnt_osago),
        case
            when sum(sales_cnt_osago) > 0 then sum(sales_discount_cnt_osago)::float / sum(sales_cnt_osago) * 100
        end as sales_discount_ratio,
        sum(sales_discount_sum_osago),
        -- Недвижимость
        sum(sales_premium_realty),
        sum(sales_cnt_realty),
        case
            when sum(sales_cnt_realty) > 0 then sum(sales_discount_cnt_realty)::float / sum(sales_cnt_realty) * 100
        end as sales_discount_ratio,
        sum(sales_discount_sum_realty),
        -- ВЗР
        sum(sales_premium_travel),
        sum(sales_cnt_travel),
        -- Уверенный водитель
        sum(sales_premium_confident_driver),
        sum(sales_cnt_confident_driver),
        -- Просто КАСКО
        sum(sales_premium_simple_kasko),
        sum(sales_cnt_simple_kasko),
        -- Пятерочка
        sum(sales_premium_raider_five),
        sum(sales_cnt_raider_five),
        -- ОСАГО+
        sum(sales_premium_raider_osago_plus),
        sum(sales_cnt_raider_osago_plus),
        -- Супер КАСКО
        sum(sales_premium_raider_super_kasko),
        sum(sales_cnt_raider_super_kasko),
        --
        'font-weight: bold; background-color: #f5f5f5;' as row_style
    from sales2 t
    cross join plan
)

select
    res.*,
    case
        when plan_ratio_policy is null then ''
        when plan_ratio_policy < 30 then 'background-color: #f2dede;'
        when plan_ratio_policy < 50 then 'background-color: #F4A460;'
        when plan_ratio_policy < 80 then 'background-color: #FFD700;'
        when plan_ratio_policy < 100 then 'background-color: #ADFF2F;'
        else 'background-color: #98FB98;'
    end as plan_ratio_policy_style
from res

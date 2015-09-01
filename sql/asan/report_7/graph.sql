with params as (
    select
        {% if not env.period_type or env.period_type == 'month' %}
            date_trunc('month', current_date) as d_start,
            current_date + interval '1 day - 1 second' as d_end,

        {% else %}
           [[env.period.0]] as d_start,
           [[env.period.1]] as d_end,

        {% endif %}
           [[env.group_by]]::varchar as trunc_by

           -- to_date('01.01.2015', 'dd.mm.yyyy') as d_start,
           -- to_date('01.05.2015', 'dd.mm.yyyy') - 1 as d_end,
           -- 'week'::varchar as trunc_by
),

-- записи о планах по периодам
plans as (
    -- select
    --     to_date('01.04.2015', 'dd.mm.yyyy') as d_start,
    --     to_date('01.05.2015', 'dd.mm.yyyy') as d_end,
    --     578.60 as comission,
    --     3869.61 as premium

    {{datasets.plans.sql}}
),

-- разворачиваем планы по дням
plan_by_days as (
    select
        dt::date as dt,
        t1.plan_comission,
        t1.plan_premium
    from (
        select
            t.comission as plan_comission,
            t.premium as plan_premium,
            generate_series(greatest(t.d_start, params.d_start),
                            least(t.d_end, params.d_end),
                            interval '1 day') as dt
        from plans t
        cross join params
        where t.d_end >= params.d_start
            and t.d_start <= params.d_end
    ) t1
    cross join params
    where extract(isodow from t1.dt) != 7
),

-- Считаем продажи
sales as (
    -- ОСАГО
    select date_trunc(params.trunc_by, t.d_issue) as dt, t.s_comission, t.s_premium
    from reports.base_osago t
    cross join params
    where t.seller_territory_id = 9
        and t.d_issue between params.d_start and params.d_end

    -- Недвижимость
    union all
    select date_trunc(params.trunc_by, t.d_issue) as dt, t.s_comission, t.s_premium
    from reports.base_realty t
    cross join params
    where t.seller_territory_id = 9
        and t.d_issue between params.d_start and params.d_end

    -- ВЗР
    union all
    select date_trunc(params.trunc_by, t.d_issue) as dt, t.s_comission, t.s_premium
    from reports.base_travel t
    cross join params
    where t.seller_territory_id = 9
        and t.d_issue between params.d_start and params.d_end

    -- Уверенный водитель
    union all
    select date_trunc(params.trunc_by, t.d_issue) as dt, t.s_comission, t.s_premium
    from reports.base_confident_driver t
    cross join params
    where t.seller_territory_id = 9
        and t.d_issue between params.d_start and params.d_end

    -- Просто КАСКО
    union all
    select date_trunc(params.trunc_by, t.d_issue) as dt, t.s_comission, t.s_premium
    from reports.base_simple_kasko t
    cross join params
    where t.seller_territory_id = 9
        and t.d_issue between params.d_start and params.d_end

    -- Пятерочка (Атешгях)
    union all
    select date_trunc(params.trunc_by, t.d_issue) as dt, t.s_comission, t.s_premium
    from reports.base_raider_five t
    cross join params
    where t.seller_territory_id = 9
        and t.d_issue between params.d_start and params.d_end

    -- Расширение ОСАГО (Атешгях)
    union all
    select date_trunc(params.trunc_by, t.d_issue) as dt, t.s_comission, t.s_premium
    from reports.base_raider_osago_plus t
    cross join params
    where t.seller_territory_id = 9
        and t.d_issue between params.d_start and params.d_end

    -- Супер КАСКО (Атешгях)
    union all
    select date_trunc(params.trunc_by, t.d_issue) as dt, t.s_comission, t.s_premium
    from reports.base_raider_super_kasko t
    cross join params
    where t.seller_territory_id = 9
        and t.d_issue between params.d_start and params.d_end
),

sales_gr as (
    select
        date_trunc(params.trunc_by, dt)::date as dt,
        sum(t.s_{{env.compare_by}}) as s_fact
        -- sum(t.s_premium) as s_fact
    from sales t
    cross join params
    group by date_trunc(params.trunc_by, dt)::date
),

-- сворачиваем по отчетному периоду
plan_by_period as (
    select
        date_trunc(params.trunc_by, t.dt)::date as dt,
        sum(t.plan_{{env.compare_by}}) as s_plan
        -- sum(t.plan_premium) as plan_premium
    from plan_by_days t
    cross join params
    where date_trunc(params.trunc_by, t.dt) between params.d_start and params.d_end
    group by date_trunc(params.trunc_by, t.dt)::date
)

select
    plan.dt,
    -- План
    plan.s_plan,
    sum(plan.s_plan) over(order by plan.dt) as s_plan_cum,
    -- Факт
    sales.s_fact,
    sum(sales.s_fact) over(order by plan.dt) as s_fact_cum
    --
from plan_by_period plan
left join sales_gr sales on plan.dt = sales.dt
order by plan.dt

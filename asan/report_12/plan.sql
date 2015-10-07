-- Параметры
with params as (
    select
        [[env.period.0]] as d_start,
        [[env.period.1]] as d_end,
        [[env.group_by]]::varchar as trunc_by,
        interval '1 {{env.group_by}}' as interv

        -- to_date('01.05.2015', 'dd.mm.yyyy') as d_start,
        -- to_date('31.05.2015', 'dd.mm.yyyy') as d_end,
        -- 'week'::varchar as trunc_by,
        -- interval '1 week' as interv

),

-- Расходы
expenses as (
    select
        t.measure,
        t.d_start as measure_month,
        t.value,
        generate_series(t.d_start, t.d_start + interval '1 month - 1 second', '1 day')::date as d_start
    from reports.rep12_plan t
),

expenses1 as (
    select
        t.d_start,
        t.measure,
        t.value::numeric / count(1) over(partition by t.measure, t.measure_month) as value_by_day,
        t.value
    from expenses t
),

expenses_measures as (
    select
        date_trunc(params.trunc_by, exp.d_start)::date as period,
        sum(case when exp.measure = 'Аренда офиса' then exp.value_by_day end) as rent_expense,
        sum(case when exp.measure = 'Канцелярия' then exp.value_by_day end) as chancellery_expense,
        max(case when exp.measure = 'Кол-во курьеров' then exp.value end) as cnt_courier,
        max(case when exp.measure = 'Кол-во операторов' then exp.value end) as cnt_operators,
        sum(case when exp.measure = 'Налоги по сотрудникам' then exp.value_by_day end) as taxes_employees_expense,
        sum(case when exp.measure = 'Прочие расходы' then exp.value_by_day end) as other_expense,
        sum(case when exp.measure = 'Расходы на курьеров' then exp.value_by_day end) as couriers_expense,
        sum(case when exp.measure = 'Телефония' then exp.value_by_day end) as phone_expense,
        sum(case when exp.measure = 'ФОТ администратора' then exp.value_by_day end) as salary_admin_expense,
        sum(case when exp.measure = 'ФОТ операторов' then exp.value_by_day end) as salary_operators,
        sum(case when exp.measure = 'Услуги уборщицы' then exp.value_by_day end) as cleaning_expense,
        sum(case when exp.measure = 'Хозяйственные принадлежности' then exp.value_by_day end) as accessories_expense
    from params
    inner join expenses1 exp on exp.d_start between params.d_start and params.d_end
    group by date_trunc(params.trunc_by, exp.d_start)
),

expenses_measures_all as (
    select
        exp.*,
        --
        rent_expense + chancellery_expense + taxes_employees_expense + other_expense
        + couriers_expense + phone_expense + salary_admin_expense
        + salary_admin_expense + accessories_expense as expens_all
    from expenses_measures exp
),

-- Доходы
incoming as (
    select
        date_trunc(params.trunc_by, t.d_issue)::date as period,
        sum(s_comission_no_discount) as s_comission_no_discount,
        sum(s_discount) as s_discount,
        sum(s_comission_no_discount) as s_comission
    from reports.base_sales t
    cross join params
    where t.d_issue between params.d_start and params.d_end
    group by date_trunc(params.trunc_by, t.d_issue)
),

-- Сводим расходы и доходы
profit as (
    select exp.*,
        incom.s_comission_no_discount,
        incom.s_discount,
        incom.s_comission,
        incom.s_comission - exp.expens_all as s_profit,
        sum(incom.s_comission - exp.expens_all) over(order by exp.period) as s_profit_cum
    from expenses_measures_all exp
    inner join incoming incom on incom.period = exp.period
),

profit_unnest as (
    select
        period,
        unnest(array['s_profit_cum', 's_profit', 'expens_all', 's_comission_no_discount', 's_discount', 's_comission', 'rent_expense', 'chancellery_expense', 'cnt_courier', 'cnt_operators', 'taxes_employees_expense', 'other_expense', 'couriers_expense', 'phone_expense', 'salary_admin_expense', 'accessories_expense']) AS measure,
        unnest(array[s_profit_cum, s_profit, expens_all, s_comission_no_discount, s_discount, s_comission, rent_expense, chancellery_expense, cnt_courier, cnt_operators, taxes_employees_expense, other_expense, couriers_expense, phone_expense, salary_admin_expense, accessories_expense]) AS value
    from profit
),

measures as (
    select 1 as num, 'sum' as op, 's_comission_no_discount' as name, 'Комиссия без учетом скидок' as title union all
    select 2 as num, 'sum' as op, 's_discount' as name, 'Размер скидок' as title union all
    select 3 as num, 'sum' as op, 's_comission' as name, 'ИТОГО ДОХОДЫ' as title union all
    select 4 as num, 'max' as op, 'cnt_operators' as name, 'Кол-во операторов' as title union all
    select 5 as num, 'sum' as op, 'salary_operators' as name, 'ФОТ операторов' as title union all
    select 6 as num, 'sum' as op, 'salary_admin_expense' as name, 'ФОТ администратора' as title union all
    select 7 as num, 'max' as op, 'cnt_courier' as name, 'Кол-во курьеров' as title union all
    select 8 as num, 'sum' as op, 'taxes_employees_expense' as name, 'Налоги по сотрудникам' as title union all
    select 9 as num, 'sum' as op, 'cleaning_expense' as name, 'Услуги уборщицы' as title union all
    select 10 as num, 'sum' as op, 'rent_expense' as name, 'Аренда офиса' as title union all
    select 11 as num, 'sum' as op, 'phone_expense' as name, 'Телефония' as title union all
    select 12 as num, 'sum' as op, 'couriers_expense' as name, 'Расходы на курьеров' as title union all
    select 13 as num, 'sum' as op, 'chancellery_expense' as name, 'Канцелярия' as title union all
    select 14 as num, 'sum' as op, 'accessories_expense' as name, 'Хозяйственные принадлежности' as title union all
    select 15 as num, 'sum' as op, 'other_expense' as name, 'Прочие расходы' as title union all
    select 16 as num, 'sum' as op, 'expens_all' as name, 'ИТОГО РАСХОДЫ' as title union all
    select 17 as num, 'sum' as op, 's_profit' as name, 'ФИН РЕЗ' as title union all
    select 18 as num, 'sum' as op, 's_profit_cum' as name, 'ФИН РЕЗ НИ' as title
)


select
    m.title,
    -- sum(case when t.period = to_date('2015-04-27', 'yyyy-mm-dd') then value end) as "2015-04-27",
    -- sum(case when t.period = to_date('2015-05-04', 'yyyy-mm-dd') then value end) as "2015-05-04",
    -- sum(case when t.period = to_date('2015-05-11', 'yyyy-mm-dd') then value end) as "2015-05-11",
    {% for row in datasets.periods.data %}
    sum(case when t.period = to_date('{{row.period}}', 'yyyy-mm-dd') then value end) as "{{row.period}}",
    {% endfor %}
    case
        when m.op = 'sum' then sum(value)
        when m.op = 'max' then max(value)
    end as "Итого"
from profit_unnest t
inner join measures m on m.name = t.measure
group by m.title, m.num, m.op
order by m.num

drop view reports.base_expense;
create or replace view reports.base_expense as

with base as (
    select
        t.period,
		t.segment,
        t.expense_type,
        t.value,
        t.period as d_from,
        (t.period + interval '1 month')::date as d_to
    from base_expense t
),

by_day as (
    select
		t.period,
        t.segment,
        t.expense_type,
        t.value as value,
        generate_series(d_from, d_to - 1, '1 day')::date as day
    from base t
),

-- Оставляем только рабочие дни
work_days as (
    select
        exp.segment,
        exp.day,
        exp.expense_type,
        exp.value / count(1) over(partition by period, exp.segment, exp.expense_type) as value
    from by_day exp
    where exp.segment = 'asan'
       or exp.segment = 'call_center'
          and extract('isodow' from exp.day) between 1 and 6
)

select * from work_days

with t1 as (
    select t.* from ({{datasets.base.sql}}) as t
)
select t2.period,
    t2.cnt,
    t2.s_insurance,
    t2.s_premium,
    null as dummy,
    t2.s_ata,
    t2.s_comission,
    t2.s_cash,
    (t2.s_comission * 0.8) + t2.s_cash as summ
from (
    select to_char(d_start, 'yyyy-mm-dd') as period,
        count(1) as cnt,
        sum(s_insurance) as s_insurance,
        sum(s_premium) as s_premium,
        sum(s_ata) as s_ata,
        sum(s_comission) as s_comission,
        sum(s_cash) as s_cash
    from t1
    where d_start >= date_trunc('month', current_date)
    group by to_char(d_start, 'yyyy-mm-dd')

    union all

    select to_char(d_start, 'yyyy-mm'),
        count(1),
        sum(s_insurance),
        sum(s_premium),
        sum(s_ata),
        sum(s_comission),
        sum(s_cash)
    from t1
    where d_start >= date_trunc('year', current_date)
    group by to_char(d_start, 'yyyy-mm')

    union all

    select to_char(d_start, 'yyyy'),
        count(1),
        sum(s_insurance),
        sum(s_premium),
        sum(s_ata),
        sum(s_comission),
        sum(s_cash)
    from t1
    group by to_char(d_start, 'yyyy')
) t2
order by substr(period, 1, 4),
    substr(period, 6, 2) = '', substr(period, 6, 2),
    substr(period, 8, 2) = '', period

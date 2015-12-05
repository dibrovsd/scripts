with t1 as (
    select
        t.user_id,
        count(1) as cnt,
        min(t.d_create) as d_create_min,
        min(case when t."path" like '/docflow/%' then t.d_create end) as d_create_min_df,
        max(t.d_create) as d_create_max
    from lib_logaccess t
    where t.d_create between [[env.d_report]] and [[env.d_report]] + interval '1 day - 1 second'
    group by t.user_id
)
select
    t1.user_id,
    t2.last_name || ' ' || t2.first_name as "Оператор",
    to_char(t1.d_create_min, 'dd.mm.yyyy hh24:mi:ss TZ') as "Первый вход",
    to_char(t1.d_create_min_df, 'dd.mm.yyyy hh24:mi:ss TZ') as "Первое действие в СЭД",
    to_char(t1.d_create_max, 'dd.mm.yyyy hh24:mi:ss TZ') as "Последнее действие",
    t1.cnt as "Кол-во"
from t1
join base_user t2 on t2.id = t1.user_id
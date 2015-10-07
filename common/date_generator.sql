-- Параметры
with params as (
    select 
        current_date - 60 as d_start, 
        current_date as d_end, 
        'week' as group_by,
        interval '1 week' as interv
),
-- Генератор дат
dates as (
    select generate_series(d_start, d_end, interv)::date as gen
    from params
)

select * from dates t
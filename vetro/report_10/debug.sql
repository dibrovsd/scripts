{{datasets.src_events.sql}},

-- Операции по дням
gr_day as (
    select
        op.m,
        op.state_id,
        op.user_id,
        date_trunc('day', op.d_create) as d_create,
        count(1) as cnt
    from operations op
    group by op.m, op.state_id, op.user_id, date_trunc('day', op.d_create)
),

-- Операции за период за день
-- чтоб нормировать операции на день
gr as (
    select
        op.m,
        op.state_id,
        op.user_id,
        sum(cnt) as cnt,
        1 as workers
    from gr_day op
    group by op.m, op.state_id, op.user_id, d_create

    union all

    select
        op.m,
        complex_state.state_id,
        op.user_id,
        sum(cnt) as cnt,
        1 as workers
    from gr_day op
    inner join complex_state on op.state_id = any (complex_state.states)
    group by op.m, complex_state.state_id, op.user_id, d_create
),

-- Итоги по юзеру
gr_cumul1 as (
    select
        gr.m,
        gr.state_id,
        gr.user_id,
        gr.cnt,
        gr.workers
    from gr

    union all

    select
        gr.m,
        gr.state_id,
        0 as user_id,
        sum(gr.cnt) as cnt,
        sum(gr.workers) as workers
    from gr
    group by gr.m, gr.state_id
),

-- Итоги по группам этапов
gr_cumul2 as (
    select
        gr.m,
        gr.state_id,
        gr.user_id,
        gr.cnt,
        gr.workers
    from gr_cumul1 gr

    union all

    select
        gr.m,
        0 as state_id,
        gr.user_id,
        sum(gr.cnt) as cnt,
        sum(gr.workers) as workers
    from gr_cumul1 gr
    where gr.state_id > 0 or gr.state_id = -3
    group by gr.m, gr.user_id

)

select * from gr_cumul1
where state_id = 7

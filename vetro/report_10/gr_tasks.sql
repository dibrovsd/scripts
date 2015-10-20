{{datasets.src_task.sql}},

-- Собираем измерение - тип_задачи - пользователь - день
-- для расчета работников (пользователь на этом направлении)
day_operations as (
    select
        op.m,
        op.tasktype_id,
        op.user_id,
        date_trunc('day', op.d_create) as d_create,
        count(1) as cnt
    from operations op
    group by op.m, op.tasktype_id, op.user_id, date_trunc('day', op.d_create)
),

gr as (
    select
        op.m,
        op.tasktype_id,
        op.user_id,
        sum(cnt) as cnt,
        1 as workers
    from day_operations op
    group by op.m, op.tasktype_id, op.user_id
),

cumul_user as (
    select
        op.m,
        user_id,
        tasktype_id,
        cnt,
        workers
    from gr op

    union all

    select
        op.m,
        0 as user_id,
        op.tasktype_id,
        sum(op.cnt) as cnt,
        sum(op.workers) as workers
    from gr op
    group by op.m, op.tasktype_id
),

cumul_type as (
    select
        op.m,
        user_id,
        tasktype_id,
        cnt,
        workers
    from cumul_user op

    union all

    select
        op.m,
        op.user_id,
        0 as tasktype_id,
        sum(op.cnt) as cnt,
        sum(op.workers) as workers
    from cumul_user op
    group by op.m, op.user_id
),

skelet as (
    select id, title from df_task_tasktype1 union all
    select 0, 'Итого'
)

select
    tt.title,
    --
    sum(case when m = 'in' and user_id = 0 then t.cnt end) as cnt_in,
    sum(case when m = 'out' and user_id = 0 then t.cnt end) as cnt_out,
    f_division(
        sum(case when m = 'out' and user_id = 0 then t.cnt end)::numeric,
        sum(case when m = 'out' and user_id = 0 then t.workers end)
    ) as out_per_day,
    --
    {% for row in datasets.users.data %}
        sum(case when m = 'in' and user_id = {{row.id}} then t.cnt end) as u{{row.id}}_cnt_in,
        sum(case when m = 'out' and user_id = {{row.id}} then t.cnt end) as u{{row.id}}_cnt_out,
        f_division(
            sum(case when m = 'out' and user_id = {{row.id}} then t.cnt end)::numeric,
            sum(case when m = 'out' and user_id = {{row.id}} then t.workers end)
        ) as u{{row.id}}_out_per_day,
    {% endfor %}
    --
    tt.id as tasktype_id
from skelet tt
join cumul_type t on t.tasktype_id = tt.id
group by tt.title, tt.id

{{datasets.src.sql}},

tasks as (
    select
        t.id,
        t.d_create,
        t.d_close,
        {% if env.show_as == 'user' %}
            t.responsible_id
        {% else %}
            d.curator_id
        {% endif %} as user_id,
        t.tasktype_id
    from df_task_task1 t
    inner join documents d on d.id = t.document_id
    where t.d_close is not null
),

operations_ as (
    select
        'in' as m,
        t.id,
        t.tasktype_id,
        t.user_id,
        t.d_create
    from tasks t
    cross join params
    where t.d_create between params.d_start and params.d_end

    union all

    select
        'out' as m,
        t.id,
        t.tasktype_id,
        t.user_id,
        t.d_close as d_create
    from tasks t
    cross join params
    where t.d_close between params.d_start and params.d_end
),

operations as (
    select op.*
    from operations_ op
    where exists (
        select null from ({{datasets.users.sql}}) u
        where u.id = op.user_id
    )
)

--------------------------------------------------------------------------------

{{datasets.src.sql}},

complex_state as (
    select -1 as state_id, array[-3, 18, 16, 15] as states union all
    select -2 as state_id, array[2, 4, 24, 5, 19] as states union all
    select -4 as state_id, array[22, 23] as states union all
    select -5 as state_id, array[6, 25] as states union all
    select -6 as state_id, array[14, 3, 21, 20, 8, 9] as states union all
    select -7 as state_id, array[10, 13] as states union all
    select -8 as state_id, array[10, 13] as states
),

-- Переходы с этапа на этап
events as (
    select
        de.id,
        de.document_id,
        de.d_create,
        d.curator_id,
        --
        coalesce(de.state_from_id, -3) as state_from_id,
        de.user_creator_id,
        --
        de.state_to_id,
        de.user_responsible_id,
        --
        row_number() over(partition by de.document_id, de.state_from_id order by de.id desc) as rn_from,
        row_number() over(partition by de.document_id, de.state_to_id order by de.id desc) as rn_to
    from docflow_documentevent1 de
    inner join documents d on d.id = de.document_id
    cross join params
    where de.d_create between params.d_start and params.d_end
    and de.state_to_id != coalesce(de.state_from_id, -3)
),

operations_ as (
    select
        'in' as m,
        e.state_to_id as state_id,
        {% if env.show_as == 'user' %}
            e.user_responsible_id
        {% else %}
            e.curator_id
        {% endif %} as user_id,
        e.document_id,
        e.id as event_id,
        e.d_create as d_create
    from events e
    where e.rn_to = 1

    union all

    select
        'out' as m,
        e.state_from_id as state_id,
        {% if env.show_as == 'user' %}
            e.user_creator_id
        {% else %}
            e.curator_id
        {% endif %} as user_id,
        e.document_id,
        e.id as event_id,
        e.d_create as d_create
    from events e
    where e.rn_from = 1

    union all

    select
        'now' as m,
        d.state_id,
        {% if env.show_as == 'user' %}
            d.responsible_id
        {% else %}
            d.curator_id
        {% endif %} as user_id,
        d.id as document_id,
        null as event_id,
        null as d_create
    from documents d
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

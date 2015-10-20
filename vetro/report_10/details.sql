{{datasets.src_operations.sql}},

op as (
    select op.* from operations op
    where 1 = 1

    {% if get.state == '0' %}

    {% elif '-' in get.state %}
        and exists (
            select null from complex_state
            where op.state_id = any (complex_state.states)
            and complex_state.state_id = [[get.state]]::integer
        )

    {% else %}
        and op.state_id = [[get.state]]::integer

    {% endif %}

    {% if get.dt == 'current' %}
        and op.m = 'now'

    {% elif get.dt == 'events_in' %}
        and op.m = 'in'

    {% elif get.dt == 'events_out' %}
        and op.m = 'out'

    {% endif %}
),

details as (

    select
        d.id,
        d.d_create::date as "Зарегистрирован",
        d.event_create::date as "Отправлен",
        d.direction_num as "Направление",
        d.inscompany as "Страховая компания",
        d.state as "Этап",
        d.city as "Город",
        d.stoa as "СТОА",
        d.stoa_company as "СТОА (компания)",
        d.responsible as "Ответственный",
        d.curator as "Куратор",
        d.damages_action as "Вид работ",
        d.replace_glass_glass_type as "Вид стекла на замену"
        {% if 'events_' in get.dt %}
            ,de.d_create as "Дата операции"
            ,st_from.title as "Откуда"
            ,st_to.title as "Куда"
        {% endif %}
    from reports.v_document d
    {% if 'events_' in get.dt %}
        inner join docflow_documentevent1 de on de.document_id = d.id
        left join docflow_state1 st_from on st_from.id = de.state_from_id
        left join docflow_state1 st_to on st_to.id = de.state_to_id
    {% endif %}
    where exists (
        select null from op
        where op.document_id = d.id

        {% if 'events_' in get.dt %}
            and op.event_id = de.id

        {% endif %}
    )
)

-- select count(1) from op
select * from details

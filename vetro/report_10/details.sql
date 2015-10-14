with params as (
    select
        [[env.city]]::integer as city,
        [[env.stoa_company]]::integer as stoa_company,
        [[env.inscompany]]::integer as inscompany,
        [[env.curator]]::integer as curator,
        [[env.period.0]]::date as d_start,
        [[env.period.1]]::date as d_end,

        [[get.state]]::integer as state_id,
        [[get.user]]::integer as user_id


        -- 0 as city,
        -- 0 as stoa_company,
        -- 0 as inscompany,
        -- 0 as role,
        -- 0 as curator,
        -- current_date - 60 as d_start,
        -- current_date as d_end
),

-- Документы
documents as (
    select d.id,
        d.responsible_id,
        d.state_id,
        --
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
    from reports.v_document d
    cross join params
    where 1 = 1
      and (params.city = 0 or d.city_auto_host_id = params.city)
      and (params.stoa_company = 0 or d.stoa_company_id = params.stoa_company)
      and (params.inscompany = 0 or d.inscompany_id = params.inscompany)
      and (params.curator = 0 or d.curator_id = params.curator)
),

-- Переходы с этапа на этап
events as (
    select
        de.id,
        de.document_id,
        de.d_create,
        --
        de.state_from_id,
        de.user_creator_id,
        --
        de.state_to_id,
        de.user_responsible_id,
        --
        row_number() over(partition by de.document_id, de.state_from_id order by de.id desc) as rn_from,
        row_number() over(partition by de.document_id, de.state_to_id order by de.id desc) as rn_to
    from docflow_documentevent1 de
    join documents d on d.id = de.document_id
    cross join params
    where de.d_create between params.d_start and params.d_end
      and exists (
          select null from documents d
          where d.id = de.document_id
      )
)

{% if get.dt == 'current' %}
    select
        d.id,
        d."Зарегистрирован",
        d."Отправлен",
        d."Направление",
        d."Страховая компания",
        d."Этап",
        d."Город",
        d."СТОА",
        d."СТОА (компания)",
        d."Ответственный",
        d."Куратор",
        d."Вид стекла на замену",
        d."Вид работ"
    from documents d
    cross join params
    where 1 = 1
      and (params.user_id = 0 or d.responsible_id = params.user_id)
      and (params.state_id = -1 or d.state_id = params.state_id)
      and exists (
          select null from ({{datasets.users.sql}}) u1
          where u1.id = d.responsible_id
      )

{% else %}
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
        d.replace_glass_glass_type as "Вид стекла на замену",
        --
        e.d_create as "Дата операции",
        st_from.title as "Откуда",
        st_to.title as "Куда"
    from events e
    join reports.v_document d on d.id = e.document_id
    left join docflow_state1 st_from on st_from.id = e.state_from_id
    left join docflow_state1 st_to on st_to.id = e.state_to_id
    cross join params
    where 1 = 1
    {% if get.dt == 'events_in' %}
        and (params.state_id = -1 or e.state_to_id = params.state_id)
        and (params.user_id = 0 or e.user_responsible_id = params.user_id)
        and e.rn_to = 1
        and exists (
            select null from ({{datasets.users.sql}}) u1
            where u1.id = e.user_responsible_id
        )

    {% elif get.dt == 'events_out' %}
        and (params.state_id = -1 or e.state_from_id = params.state_id)
        and (params.user_id = 0 or e.user_creator_id = params.user_id)
        and e.rn_from = 1
        and exists (
            select null from ({{datasets.users.sql}}) u1
            where u1.id = e.user_creator_id
        )

    {% endif %}

{% endif %}

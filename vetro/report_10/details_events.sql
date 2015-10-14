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
        d.state_id
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
        de.d_create,
        --
        de.state_from_id,
        de.user_creator_id,
        --
        de.state_to_id,
        de.user_responsible_id,
        --
        row_number() over(partition by de.document_id, de.state_from_id order by id desc) as rn_from,
        row_number() over(partition by de.document_id, de.state_to_id order by id desc) as rn_to
    from docflow_documentevent1 de
    cross join params
    where de.d_create between params.d_start and params.d_end
      and exists (
          select null from documents d
          where d.id = de.document_id
      )
)

{% if get.dt == 'current' %}
    select count(1)
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
    select count(1)
    from events e
    cross join params
    where 1 = 1
    {% if get.dt == 'events_in' %}
        and (params.state_id = -1 or e.state_to_id = params.state_id)
        and (params.user_id = 0 or e.user_responsible_id = params.user_id)
        and e.rn_to = 1

    {% elif get.dt == 'events_out' %}
        and (params.state_id = -1 or e.state_from_id = params.state_id)
        and (params.user_id = 0 or e.user_creator_id = params.user_id)
        and e.rn_from = 1

    {% endif %}

{% endif %}

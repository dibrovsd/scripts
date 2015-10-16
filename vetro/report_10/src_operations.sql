with params as (
    select
        [[env.city]]::integer as city,
        [[env.stoa_company]]::integer as stoa_company,
        [[env.inscompany]]::integer as inscompany,
        [[env.period.0]] as d_start,
        [[env.period.1]] as d_end

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
        d.curator_id,
        d.state_id
    from reports.v_document d
    cross join params
    where 1 = 1
      and (params.city = 0 or d.city_auto_host_id = params.city)
      and (params.stoa_company = 0 or d.stoa_company_id = params.stoa_company)
      and (params.inscompany = 0 or d.inscompany_id = params.inscompany)
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

operations as (
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
)

--------------------------------------------------------------------------------
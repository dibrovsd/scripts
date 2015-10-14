with params as (
    select
        [[env.city]]::integer as city,
        [[env.stoa_company]]::integer as stoa_company,
        [[env.inscompany]]::integer as inscompany,
        [[env.curator]]::integer as curator,
        [[env.period.0]]::date as d_start,
        [[env.period.1]]::date as d_end

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
),

-- Последнее событие по документу, полученное оператором
events_in as (
    select
        e.state_to_id as state_id,
        e.user_responsible_id as user_id
    from events e
    where e.rn_to = 1
),

-- Последнее событие по документу, отправленное оператором
events_out as (
    select
        e.state_from_id as state_id,
        e.user_creator_id as user_id,
        e.d_create
    from events e
    where e.rn_from = 1
)

select
    u.id,
    u.last_name || ' ' || u.first_name as title
from base_user u
where (
    exists (
        select null from events_in e
        where e.user_id = u.id
    )
    or exists (
        select null from events_out e
        where e.user_id = u.id
    )
    or exists (
        select null from documents d
        where d.responsible_id = u.id
    )
)


{% if env.role != 0 %}
    and exists (
        select null from docflow_projectuser1 pu
        where pu.user_id = u.id
        and exists (
            select null from docflow_projectuser1_roles pur
            where pur.projectuser1_id = pu.id
            and pur.role1_id = [[env.role]]
        )
    )
{% endif %}
order by title

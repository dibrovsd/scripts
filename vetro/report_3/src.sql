with params as (
    select

    [[env.city_auto_host]]::integer as city_auto_host,
    [[env.direction_stoa]]::integer as direction_stoa,
    [[env.responsible]]::integer as responsible,
    [[env.handling_type]]::integer as handling_type,
    [[env.curator]]::integer as curator


    -- 0 as city_auto_host,
    -- 0 as direction_stoa,
    -- 0 as responsible
),

documents as (
    select d.*
    from reports.v_document d
    cross join params
    where (params.city_auto_host = 0 or d.city_auto_host_id = params.city_auto_host)
      and (params.direction_stoa = 0 or d.stoa_id = params.direction_stoa)
      and (params.responsible = 0 or d.responsible_id = params.responsible)
      and (params.curator = 0 or d.curator_id = params.curator)
      and (params.handling_type = 0 or d.handling_type_id = params.handling_type)

      {% if 'customer_service' in user_params.roles %}
         and d.curator_id = {{user.id}}

      {% elif 'stoa' in user_params.roles %}
         and d.stoa_id in ({{user.stations_ids|join:","}})

      {% endif %}
),

measures as (
    select
        'insp' as measure,
        d.id,
        d.inspection_date::date as m_date,
        current_date - d.inspection_date::date as days_expire
    from documents d
    where d.inspection_date is not null
      and d.state_id = 3 -- Осмотр ТС

    union all

    select
        'repair' as measure,
        d.id,
        d.repair_date::date as m_date,
        current_date - d.repair_date::date as days_expire
    from documents d
    where d.repair_date is not null
      and d.state_id = 8 -- Ожидание клиента

    union all

    select
        'remote' as measure,
        d.id,
        d.incoming_date::date as m_date,
        current_date - d.incoming_date::date as days_expire
    from documents d
    where d.incoming_date is not null
      and d.state_id = 20 -- Прием документов УУ

)

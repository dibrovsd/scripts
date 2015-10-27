with params as (
    select
        [[env.city_auto_host]]::integer as city_auto_host,
        [[env.direction_stoa]]::integer as direction_stoa,
        [[env.responsible]]::integer as responsible,
        [[env.handling_type]]::integer as handling_type,
        [[env.inscompany]]::integer as inscompany,
        [[env.curator]]::integer as curator

        -- 0 as city_auto_host,
        -- 0 as direction_stoa,
        -- 0 as inscompany,
        -- 0 as responsible
),

documents_base as (
    select
        d.id,
        d.d_create,
        d.event_create as d_create_event,
        d.state_id,
        case
            -- Согласование стекла с клиентом
            when d.state_id = 4 then d.glass_in_stock
            else ''
        end as glass_in_stock
    from reports.v_document d
    cross join params
    where (params.city_auto_host = 0 or d.city_auto_host_id = params.city_auto_host)
      and (params.direction_stoa = 0 or d.stoa_id = params.direction_stoa)
      and (params.responsible = 0 or d.responsible_id = params.responsible)
      and (params.inscompany = 0 or d.inscompany_id = params.inscompany)
      and (params.curator = 0 or d.curator_id = params.curator)
      and (params.handling_type = 0 or d.handling_type_id = params.handling_type)
      and d.state_id != 12

      {% if not user_params.roles %}
        and (params.curator = 0 or d.curator_id = params.curator)

      {% elif 'customer_service' in user_params.roles %}
         and d.curator_id = {{user.id}}

      {% elif 'stoa' in user_params.roles %}
         and d.stoa_id in ({{user.stations_ids|join:","}})
         -- "Ожидание решения СК о смене СТОА" и "Ожидание оплаты"
         and d.state_id not in (15, 11)
      {% endif %}

),

-- Задачи
tasks as (
    select
        tsk.document_id,
        tsk.tasktype_id,
        tsk.state,
        tsk.d_create::date as d_start,
        tsk.d_close::date as d_close,
        coalesce(tsk.d_close, current_date)::date as d_end
    from df_task_task1 tsk
    join documents_base d on d.id = tsk.document_id
    where tsk.tasktype_id in (1,2,3)
        and tsk.state != 3
),

tasks1 as (
    select
        tasks.*,
        d_end - d_start as days
    from tasks
),

-- Документы
documents as (
    select
        d.id,
        coalesce(d.d_create_event, d.d_create)::date as d_start,
        coalesce(d.state_id, 0) as state_id,
        d.glass_in_stock,
        current_date as d_end
    from documents_base d
),

documents1 as (
    select
        documents.*,
        d_end - d_start as days
    from documents
)

with params as (
    select
        -- [[env.city]]::integer as city,
        -- [[env.stoa_company]]::integer as stoa_company,
        -- [[env.inscompany]]::integer as inscompany,
        -- [[env.curator]]::integer as curator,
        -- [[env.period.0]]::date as d_start,
        -- [[env.period.1]]::date as d_end

        0 as city,
        0 as stoa_company,
        0 as inscompany,
        0 as role,
        0 as curator,
        current_date - 60 as d_start,
        current_date as d_end
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

-- Задачи
tasks as (
    select
        t.tasktype_id,
        t.responsible_id as user_id,
        t.d_create,
        t.d_close
    from df_task_task1 t
    cross join params
    where t.state = 3
    and exists (
        select null from documents d
        where d.id = t.document_id
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
),

-- Вход на этап
events_in_gr as (
    select
        state_id,
        user_id,
        count(1) as cnt
    from events_in
    group by state_id, user_id
),

-- Выход с этапа (по дням)
events_out_gr_day as (
    select
        state_id,
        user_id,
        date_trunc('day', d_create) as day,
        count(1) as cnt
    from events_out
    group by state_id, user_id, date_trunc('day', d_create)
),

-- Выход с этапа
events_out_gr as (
    select
        state_id,
        user_id,
        sum(e.cnt) as cnt,
        round(avg(e.cnt), 2) as cnt_per_day
    from events_out_gr_day e
    group by state_id, user_id
),

-- Текущее кол-во документов на операторе
current_gr as (
    select
        d.state_id,
        d.responsible_id as user_id,
        count(1) as cnt
    from documents d
    group by d.state_id, d.responsible_id
),

tsk_in as (
    select
        t.tasktype_id,
        t.user_id,
        count(1) as cnt
    from tasks t
    cross join params
    where t.d_create between params.d_start and params.d_end
    group by t.tasktype_id, t.user_id
),

tsk_out as (
    select
        t.tasktype_id,
        t.user_id,
        count(1) as cnt
    from tasks t
    cross join params
    where t.d_close between params.d_start and params.d_end
    group by t.tasktype_id, t.user_id
),

-- Данные по этапам
by_state as (
    select
        u.last_name || ' '|| u.first_name as user,
        st.title as state,
        events_in.cnt as events_in,
        events_out.cnt as events_out,
        events_out.cnt_per_day as events_out_per_day,
        current_gr.cnt as current
    from base_user u
    cross join docflow_state1 st
    left join events_in_gr events_in on events_in.state_id = st.id and events_in.user_id = u.id
    left join events_out_gr events_out on events_out.state_id = st.id and events_out.user_id = u.id
    left join current_gr on current_gr.state_id = st.id and current_gr.user_id = u.id
    where events_in.cnt is not null
      or events_out.cnt is not null
),

by_task as (
    select
        u.last_name || ' '|| u.first_name as user,
        tsk_type.title as state,
        tsk_in.cnt,
        tsk_out.cnt
    from base_user u
    cross join df_task_tasktype1 tsk_type
    left join tsk_in on tsk_in.tasktype_id = tsk_type.id and tsk_in.user_id = u.id
    left join tsk_out on tsk_out.tasktype_id = tsk_type.id and tsk_out.user_id = u.id
    where tsk_in.cnt is not null or tsk_out.cnt is not null
)

select * from by_task

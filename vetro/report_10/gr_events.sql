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
        count(1) as cnt_days
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

-- Данные по этапам
by_state_base as (
    select
        u.id as user_id,
        st.id as state_id,
        u.last_name || ' '|| u.first_name as user,
        st.title as state,
        events_in.cnt as events_in,
        events_out.cnt as events_out,
        events_out.cnt_days as events_out_days,
        current_gr.cnt as current
    from base_user u
    cross join docflow_state1 st
    left join events_in_gr events_in on events_in.state_id = st.id and events_in.user_id = u.id
    left join events_out_gr events_out on events_out.state_id = st.id and events_out.user_id = u.id
    left join current_gr on current_gr.state_id = st.id and current_gr.user_id = u.id
    where events_in.cnt is not null
      or events_out.cnt is not null
      and exists (
          select null from ({{datasets.users.sql}}) u1
          where u1.id = u.id
      )
),

by_state as (
    select
        user_id,
        state_id,
        user,
        state,
        events_in,
        events_out,
        events_out_days,
        current
    from by_state_base

    union all

    select
        user_id,
        -1 as state_id,
        user,
        null as state,
        sum(events_in) as events_in,
        sum(events_out) as events_out,
        sum(events_out_days) as events_out_days,
        sum(current) as current
    from by_state_base
    group by user_id, user
),

skelet as (
    select 'Регистрация направления' as title, null as id, 1 as n_order, 'background-color: #ccc; font-weight: bold;' as row_style union all
    select 'Регистрация направления' as title, 0 as id, 2 as n_order, '' as row_style union all
    select 'Регистрация и первичный звонок клиенту по УУ' as title, 18 as id, 3 as n_order, '' as row_style union all
    select 'Уточнение контактного номер телефона' as title, 16 as id, 4 as n_order, '' as row_style union all
    select 'Ожидание решения СК о смене СТОА' as title, 15 as id, 5 as n_order, '' as row_style union all

    select 'Экспертная работа' as title, null as id, 6 as n_order, 'background-color: #ccc; font-weight: bold;' as row_style union all
    select 'Приглашение на осмотр' as title, 2 as id, 7 as n_order, '' as row_style union all
    select 'Согласование стекла с клиентом' as title, 4 as id, 8 as n_order, '' as row_style union all
    select 'Определение типа стекла по УУ' as title, 24 as id, 9 as n_order, '' as row_style union all
    select 'Согласование ПЗН' as title, 5 as id, 10 as n_order, '' as row_style union all
    select 'Приглашение на СТОА по УУ' as title, 19 as id, 11 as n_order, '' as row_style union all

    select 'Телефонное обслуживание' as title, null as id, 12 as n_order, 'background-color: #ccc; font-weight: bold;' as row_style union all
    select 'Приглашение клиента на ремонт' as title, 26 as id, 13 as n_order, '' as row_style union all

    select 'Страховое событие' as title, null as id, 14 as n_order, 'background-color: #ccc; font-weight: bold;' as row_style union all
    select 'Согласование УУ со стороны СК' as title, 23 as id, 15 as n_order, '' as row_style union all
    select 'Принятие решение по УУ РАВТ' as title, 22 as id, 16 as n_order, '' as row_style union all

    select 'Закупка стекла и резервирование' as title, null as id, 17 as n_order, 'background-color: #ccc; font-weight: bold;' as row_style union all
    select 'Ожидание закупки стекла' as title, 25 as id, 18 as n_order, '' as row_style union all
    select 'Акцептование ремонта на СТОА' as title, 6 as id, 19 as n_order, '' as row_style union all

    select 'СТОА' as title, null as id, 19.1 as n_order, 'background-color: #ccc; font-weight: bold;' as row_style union all
    select 'Запрос наличия стекла' as title, 14 as id, 20 as n_order, '' as row_style union all
    select 'Осмотр ТС' as title, 3 as id, 21 as n_order, '' as row_style union all
    select 'Прием документов УУ' as title, 20 as id, 22 as n_order, '' as row_style union all
    select 'Ожидание документов УУ' as title, 21 as id, 23 as n_order, '' as row_style union all
    select 'Ожидание клиента' as title, 8 as id, 24 as n_order, '' as row_style union all
    select 'Укомплектование дела' as title, 9 as id, 25 as n_order, '' as row_style union all

    select 'Сдача документов в СК' as title, null as id, 26 as n_order, 'background-color: #ccc; font-weight: bold;' as row_style union all
    select 'Генерация счета' as title, 10 as id, 27 as n_order, '' as row_style union all
    select 'Передача оригиналов в СК' as title, 13 as id, 28 as n_order, '' as row_style union all

    select 'Бухгалтерия' as title, null as id, 29 as n_order, 'background-color: #ccc; font-weight: bold;' as row_style union all
    select 'Ожидание оплаты' as title, 11 as id, 30 as n_order, '' as row_style union all

    select 'Итого' as title, -1 as id, 31 as n_order, 'background-color: #ccc; font-weight: bold;' as row_style
)

select
    skelet.title,
    --
    sum(dt.events_in) as events_in,
    sum(dt.events_out) as events_out,
    sum(dt.current) as current,
    f_division(sum(dt.events_out), sum(dt.events_out_days)) as events_out_per_day,
    --
    -- max(case when dt.user_id = 54 then dt.events_in end) as u30_events_in,
    -- max(case when dt.user_id = 54 then dt.events_out end) as u30_events_out,
    -- max(case when dt.user_id = 54 then dt.events_out_per_day end) as u30_events_out_per_day,
    -- max(case when dt.user_id = 54 then dt.current end) as u30_current,
    /*
    */
    {% for row in datasets.users.data %}
        max(case when dt.user_id = {{row.id}} then dt.events_in end) as u{{row.id}}_events_in,
        max(case when dt.user_id = {{row.id}} then dt.events_out end) as u{{row.id}}_events_out,
        max(case when dt.user_id = {{row.id}} then dt.current end) as u{{row.id}}_current,
        f_division(
            max(case when dt.user_id = {{row.id}} then dt.events_out end),
            max(case when dt.user_id = {{row.id}} then dt.events_out_days end)
        ) as u{{row.id}}_events_out_per_day,
    {% endfor %}
    --
    skelet.id as state_id,
    skelet.row_style
from skelet
left join by_state dt on dt.state_id = skelet.id
group by skelet.title, skelet.id, skelet.n_order, skelet.row_style
order by skelet.n_order

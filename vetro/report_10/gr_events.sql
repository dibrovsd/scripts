{{datasets.src_events.sql}},

-- Операции по дням
gr_day as (
    select
        op.m,
        op.state_id,
        op.user_id,
        date_trunc('day', op.d_create) as d_create,
        count(1) as cnt
    from operations op
    group by op.m, op.state_id, op.user_id, date_trunc('day', op.d_create)
),

-- Операции за период (нормированные по дню операции)
gr as (
    select
        op.m,
        op.state_id,
        op.user_id,
        sum(cnt) as cnt,
        1 as workers
    from gr_day op
    group by op.m, op.state_id, op.user_id, d_create

    union all

    select
        op.m,
        complex_state.state_id,
        op.user_id,
        sum(cnt) as cnt,
        1 as workers
    from gr_day op
    inner join complex_state on op.state_id = any (complex_state.states)
    group by op.m, complex_state.state_id, op.user_id, d_create
),

-- Итоги по юзеру
gr_cumul1 as (
    select
        gr.m,
        gr.state_id,
        gr.user_id,
        gr.cnt,
        gr.workers
    from gr

    union all

    select
        gr.m,
        gr.state_id,
        0 as user_id,
        sum(gr.cnt) as cnt,
        sum(gr.workers) as workers
    from gr
    group by gr.m, gr.state_id
),

-- Итоги по группам этапов
gr_cumul2 as (
    select
        gr.m,
        gr.state_id,
        gr.user_id,
        gr.cnt,
        gr.workers
    from gr_cumul1 gr

    union all

    select
        gr.m,
        0 as state_id,
        gr.user_id,
        sum(gr.cnt) as cnt,
        sum(gr.workers) as workers
    from gr_cumul1 gr
    where gr.state_id > 0 or gr.state_id = -3
    group by gr.m, gr.user_id

),

skelet as (
    select 'Регистрация направления' as title, -1 as id, 1 as n_order, 'background-color: #ccc; font-weight: bold;' as row_style union all
    select 'Регистрация направления' as title, -3 as id, 2 as n_order, '' as row_style union all
    select 'Регистрация и первичный звонок клиенту по УУ' as title, 18 as id, 3 as n_order, '' as row_style union all
    select 'Уточнение контактного номер телефона' as title, 16 as id, 4 as n_order, '' as row_style union all
    select 'Ожидание решения СК о смене СТОА' as title, 15 as id, 5 as n_order, '' as row_style union all

    select 'Экспертная работа' as title, -2 as id, 6 as n_order, 'background-color: #ccc; font-weight: bold;' as row_style union all
    select 'Приглашение на осмотр' as title, 2 as id, 7 as n_order, '' as row_style union all
    select 'Согласование стекла с клиентом' as title, 4 as id, 8 as n_order, '' as row_style union all
    select 'Определение типа стекла по УУ' as title, 24 as id, 9 as n_order, '' as row_style union all
    select 'Согласование ПЗН' as title, 5 as id, 10 as n_order, '' as row_style union all
    select 'Приглашение на СТОА по УУ' as title, 19 as id, 11 as n_order, '' as row_style union all

    select 'Телефонное обслуживание' as title, 7 as id, 12 as n_order, 'background-color: #ccc; font-weight: bold;' as row_style union all
    select 'Приглашение клиента на ремонт' as title, 7 as id, 13 as n_order, '' as row_style union all

    select 'Страховое событие' as title, -4 as id, 14 as n_order, 'background-color: #ccc; font-weight: bold;' as row_style union all
    select 'Согласование УУ со стороны СК' as title, 23 as id, 15 as n_order, '' as row_style union all
    select 'Принятие решение по УУ РАВТ' as title, 22 as id, 16 as n_order, '' as row_style union all

    select 'Закупка стекла и резервирование' as title, -5 as id, 17 as n_order, 'background-color: #ccc; font-weight: bold;' as row_style union all
    select 'Ожидание закупки стекла' as title, 25 as id, 18 as n_order, '' as row_style union all
    select 'Акцептование ремонта на СТОА' as title, 6 as id, 19 as n_order, '' as row_style union all

    select 'СТОА' as title, -6 as id, 19.1 as n_order, 'background-color: #ccc; font-weight: bold;' as row_style union all
    select 'Запрос наличия стекла' as title, 14 as id, 20 as n_order, '' as row_style union all
    select 'Осмотр ТС' as title, 3 as id, 21 as n_order, '' as row_style union all
    select 'Прием документов УУ' as title, 20 as id, 22 as n_order, '' as row_style union all
    select 'Ожидание документов УУ' as title, 21 as id, 23 as n_order, '' as row_style union all
    select 'Ожидание клиента' as title, 8 as id, 24 as n_order, '' as row_style union all
    select 'Укомплектование дела' as title, 9 as id, 25 as n_order, '' as row_style union all

    select 'Сдача документов в СК' as title, -7 as id, 26 as n_order, 'background-color: #ccc; font-weight: bold;' as row_style union all
    select 'Генерация счета' as title, 10 as id, 27 as n_order, '' as row_style union all
    select 'Передача оригиналов в СК' as title, 13 as id, 28 as n_order, '' as row_style union all

    select 'Бухгалтерия' as title, 11 as id, 29 as n_order, 'background-color: #ccc; font-weight: bold;' as row_style union all
    select 'Ожидание оплаты' as title, 11 as id, 30 as n_order, '' as row_style union all

    select 'Итого' as title, 0 as id, 31 as n_order, 'background-color: #ccc; font-weight: bold;' as row_style
)
--
-- select sum(gr.cnt), sum(workers) from gr
-- where m = 'out'
-- and state_id = -6
--
select
    skelet.title,
    --
    sum(case when dt.m = 'in' and dt.user_id = 0 then dt.cnt end) as events_in,
    sum(case when dt.m = 'out' and dt.user_id = 0 then dt.cnt end) as events_out,
    sum(case when dt.m = 'now' and dt.user_id = 0 then dt.cnt end) as current,
    f_division(
        sum(case when dt.m = 'out' and dt.user_id = 0 then dt.cnt end)::numeric,
        sum(case when dt.m = 'out' and dt.user_id = 0 then dt.workers end)
    ) as events_out_per_day,
    --
    {% for row in datasets.users.data %}
    sum(case when dt.m = 'in' and dt.user_id = {{row.id}} then dt.cnt end) as u{{row.id}}_events_in,
    sum(case when dt.m = 'out' and dt.user_id = {{row.id}} then dt.cnt end) as u{{row.id}}_events_out,
    sum(case when dt.m = 'now' and dt.user_id = {{row.id}} then dt.cnt end) as u{{row.id}}_current,
    f_division(
        sum(case when dt.m = 'out' and dt.user_id = {{row.id}} then dt.cnt end)::numeric,
        sum(case when dt.m = 'out' and dt.user_id = {{row.id}} then dt.workers end)
    ) as u{{row.id}}_events_out_per_day,
    {% endfor %}
    skelet.row_style,
    skelet.id as state_id
from skelet
left join gr_cumul2 dt on dt.state_id = skelet.id
group by skelet.id, skelet.title, skelet.row_style, skelet.n_order
order by skelet.n_order

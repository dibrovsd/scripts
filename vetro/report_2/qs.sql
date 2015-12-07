{{datasets.src.sql}},

tasks_gr as (
    select
        tasktype_id,
        state,
        round(avg(days)) as days,
        sum(days) as days_sum,
        count(1) as cnt,
        count(case when days between 0 and 2 then 1 end) as cnt_0_2,
        count(case when days between 3 and 5 then 1 end) as cnt_3_5,
        count(case when days between 6 and 10 then 1 end) as cnt_6_10,
        count(case when days between 11 and 20 then 1 end) as cnt_11_20,
        count(case when days > 20 then 1 end) as cnt_20
    from tasks1
    group by tasktype_id, state

    union all

    select
        tasktype_id,
        0 as state,
        round(avg(days)) as days,
        sum(days) as days_sum,
        count(1) as cnt,
        count(case when days between 0 and 2 then 1 end) as cnt_0_2,
        count(case when days between 3 and 5 then 1 end) as cnt_3_5,
        count(case when days between 6 and 10 then 1 end) as cnt_6_10,
        count(case when days between 11 and 20 then 1 end) as cnt_11_20,
        count(case when days > 20 then 1 end) as cnt_20
    from tasks1
    group by tasktype_id
),


documents_gr_ as (
    select
        state_id,
        state_measure,
        sum(days) as days_sum,
        count(1) as cnt,
        count(case when days between 0 and 2 then 1 end) as cnt_0_2,
        count(case when days between 3 and 5 then 1 end) as cnt_3_5,
        count(case when days between 6 and 10 then 1 end) as cnt_6_10,
        count(case when days between 11 and 20 then 1 end) as cnt_11_20,
        count(case when days > 20 then 1 end) as cnt_20
    from documents1
    group by state_id, state_measure
),

-- Добавляем итоги по glass_in_stock для 4 этапа
documents_gr as (
    select
        d.state_id,
        d.state_measure,
        f_division(d.days_sum, d.cnt) as days,
        d.days_sum,
        d.cnt,
        d.cnt_0_2,
        d.cnt_3_5,
        d.cnt_6_10,
        d.cnt_11_20,
        d.cnt_20
    from documents_gr_ d

    union all

    select
        d.state_id,
        '' as state_measure,
        f_division(sum(d.days_sum), sum(d.cnt)) as days,
        sum(d.days_sum),
        sum(d.cnt),
        sum(d.cnt_0_2),
        sum(d.cnt_3_5),
        sum(d.cnt_6_10),
        sum(d.cnt_11_20),
        sum(d.cnt_20)
    from documents_gr_ d
    where d.state_id in (4,2,7)
    group by d.state_id
),

measures_ as (
    select 'Регистрация направления ' as title, null::integer as doc_state, null::integer as task_type, null::integer as task_state, '' as state_measure union all
    select 'Регистрация направления' as title, 0 as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select 'Регистрация и первичный звонок клиенту по УУ' as title, 18 as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select 'Уточнение контактного номер телефона' as title, 16 as doc_state, null as task_type, null as task_state, '' as state_measure union all
    {% if not 'stoa' in user_params.roles %}
        select 'Ожидание решения СК о смене СТОА' as title, 15 as doc_state, null as task_type, null as task_state, '' as state_measure union all
    {% endif %}

    select 'Экспертная работа' as title, null as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select 'Приглашение на осмотр' as title, 2 as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select '&nbsp;- есть задача перезвонить' as title, 2 as doc_state, null as task_type, null as task_state, 'true' as state_measure union all
    select '&nbsp;- нет задачи перезвонить' as title, 2 as doc_state, null as task_type, null as task_state, 'false' as state_measure union all


    select 'Согласование стекла с клиентом' as title, 4 as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select '&nbsp;- На складе' as title, 4 as doc_state, null as task_type, null as task_state, 'Есть' as state_measure union all
    select '&nbsp;- Закупка' as title, 4 as doc_state, null as task_type, null as task_state, 'Нет' as state_measure union all
    select 'Определение типа стекла по УУ' as title, 24 as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select 'Согласование ПЗН' as title, 5 as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select 'Приглашение на СТОА по УУ' as title, 19 as doc_state, null as task_type, null as task_state, '' as state_measure union all

    select 'Телефонное обслуживание' as title, null as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select 'Приглашение клиента на ремонт' as title, 7 as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select '&nbsp;- есть задача перезвонить' as title, 7 as doc_state, null as task_type, null as task_state, 'true' as state_measure union all
    select '&nbsp;- нет задачи перезвонить' as title, 7 as doc_state, null as task_type, null as task_state, 'false' as state_measure union all

    select 'Страховое событие' as title, null as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select 'Согласование УУ со стороны СК' as title, 23 as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select 'Принятие решение по УУ РАВТ' as title, 22 as doc_state, null as task_type, null as task_state, '' as state_measure union all

    select 'Закупка стекла и резервирование' as title, null as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select 'Ожидание закупки стекла' as title, 25 as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select 'Акцептование ремонта на СТОА' as title, 6 as doc_state, null as task_type, null as task_state, '' as state_measure union all

    select 'Резервирование стекла (задача)' as title, null as doc_state, 1 as task_type, 0 as task_state, '' as state_measure union all
    select '- Ожидание' as title, null as doc_state, 1 as task_type, 1 as task_state, '' as state_measure union all

    select 'Закупка стекла РАВТ (задача)' as title, null as doc_state, 3 as task_type, 0 as task_state, '' as state_measure union all
    select '- Ожидание' as title, null as doc_state, 3 as task_type, 1 as task_state, '' as state_measure union all
    select '- Заказано стекло' as title, null as doc_state, 3 as task_type, 5 as task_state, '' as state_measure union all
    select '- Стекло в пути' as title, null as doc_state, 3 as task_type, 2 as task_state, '' as state_measure union all

    select 'Закупка стекла СТОА (задача)' as title, null as doc_state, 2 as task_type, 0 as task_state, '' as state_measure union all
    select '- Ожидание' as title, null as doc_state, 2 as task_type, 1 as task_state, '' as state_measure union all
    select '- Заказано стекло' as title, null as doc_state, 2 as task_type, 5 as task_state, '' as state_measure union all

    select 'СТОА' as title, null as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select 'Запрос наличия стекла' as title, 14 as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select 'Осмотр ТС' as title, 3 as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select 'Прием документов УУ' as title, 20 as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select 'Ожидание документов УУ' as title, 21 as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select 'Ожидание клиента' as title, 8 as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select 'Укомплектование дела' as title, 9 as doc_state, null as task_type, null as task_state, '' as state_measure union all

    select 'Сдача документов в СК' as title, null as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select 'Генерация счета' as title, 10 as doc_state, null as task_type, null as task_state, '' as state_measure union all
    select 'Передача оригиналов в СК' as title, 13 as doc_state, null as task_type, null as task_state, '' as state_measure union all

    select 'Бухгалтерия' as title, null as doc_state, null as task_type, null as task_state, '' as state_measure
    {% if not 'stoa' in user_params.roles %}
    union all select 'Ожидание оплаты' as title, 11 as doc_state, null as task_type, null as task_state, '' as state_measure
    {% endif %}

),

measures as (
    select m.*, row_number() over() as num
    from measures_ m
)

select m.title,
    coalesce(d.days, tsk.days) as days,
    coalesce(d.cnt, tsk.cnt) as cnt,
    coalesce(d.cnt_0_2, tsk.cnt_0_2) as cnt_0_2,
    coalesce(d.cnt_3_5, tsk.cnt_3_5) as cnt_3_5,
    coalesce(d.cnt_6_10, tsk.cnt_6_10) as cnt_6_10,
    coalesce(d.cnt_11_20, tsk.cnt_11_20) as cnt_11_20,
    coalesce(d.cnt_20, tsk.cnt_20) as cnt_20,
    case
        when m.title in ('Регистрация направления ', 'СТОА', 'Экспертная работа', 'Телефонное обслуживание', 'Страховое событие', 'Закупка стекла и резервирование', 'Сдача документов в СК', 'Бухгалтерия')
        then 'font-weight: bold; background-color: #f5f5f5;'
        when m.title in ('Закупка стекла РАВТ (задача)', 'Резервирование стекла (задача)', 'Закупка стекла СТОА (задача)')
        then 'font-weight: bold;'
    end as row_style,
    m.num,
    case
        when m.doc_state is not null
            then 'document'
        when m.task_type is not null
            then 'task'
    end as m_type,
    m.doc_state,
    m.task_type,
    m.task_state,
    m.state_measure
from measures m
left join documents_gr d on d.state_id = m.doc_state and d.state_measure = m.state_measure
left join tasks_gr tsk on tsk.tasktype_id = m.task_type
                       and tsk.state = m.task_state

union all

select 'Итого документы' as title,
    round(sum(d.days_sum) / sum(d.cnt)) as days,
    sum(d.cnt) as cnt,
    sum(d.cnt_0_2) as cnt_0_2,
    sum(d.cnt_3_5) as cnt_3_5,
    sum(d.cnt_6_10) as cnt_6_10,
    sum(d.cnt_11_20) as cnt_11_20,
    sum(d.cnt_20) as cnt_20,
    'font-weight: bold;' as row_style,
    --
    100 as num,
    'document' as m_type,
    null as doc_state,
    null as task_type,
    null as task_state,
    '' as state_measure
from documents_gr d
where d.state_id is not null
  and d.state_id != 12

union all

select 'Итого задачи' as title,
    round(sum(tsk.days_sum) / sum(tsk.cnt)) as days,
    sum(tsk.cnt) as cnt,
    sum(tsk.cnt_0_2) as cnt_0_2,
    sum(tsk.cnt_3_5) as cnt_3_5,
    sum(tsk.cnt_6_10) as cnt_6_10,
    sum(tsk.cnt_11_20) as cnt_11_20,
    sum(tsk.cnt_20) as cnt_20,
    'font-weight: bold;' as row_style,
    101 as num,
    'task' as m_type,
    null as doc_state,
    null as task_type,
    null as task_state,
    '' as state_measure
from measures m
inner join tasks_gr tsk on tsk.tasktype_id = m.task_type and tsk.state = 0 and m.task_state = 0

order by num

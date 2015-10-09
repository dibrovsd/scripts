select
    'Создано задач' as title,
    count(1) as value
from ({{datasets.base.sql}}) t

union all

select
    'Перезвонить' as title,
    count(1) as value
from ({{datasets.base.sql}}) t
where exists (
    select null from crm_calltasklog log
    where log.task_id = t.id
    and log.status = 11 -- Перезвонить
)


union all

select
    'Продажа' as title,
    count(1) as value
from ({{datasets.base.sql}}) t
where exists (
    select null from crm_calltasklog log
    where log.task_id = t.id
    and log.status = 11 -- Перезвонить
)
and t.status = 1 -- Успешная продажа

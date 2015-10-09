select
    'В отчетном периоде' as title,
    count(1) as value
from ({{datasets.base.sql}}) t

union all

select
    'Ожидание документов' as title,
    count(1) as value
from ({{datasets.base.sql}}) t
where exists (
    select null from crm_calltasklog log
    where log.task_id = t.id
    and log.status = 12 -- Ожидание документов
)


union all

select
    'Продажа' as title,
    count(1) as value
from ({{datasets.base.sql}}) t
where exists (
    select null from crm_calltasklog log
    where log.task_id = t.id
    and log.status = 12 -- Ожидание документов
)
and t.status = 1 -- Успешная продажа

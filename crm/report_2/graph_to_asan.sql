select
    'Направлен в АСАН' as title,
    count(1) as value
from ({{datasets.base.sql}}) t
where exists (
    select null from crm_calltasklog log
    where log.task_id = t.id
    and log.status = 17
)

union all

select
    'Продажа' as title,
    count(1) as value
from ({{datasets.base.sql}}) t
where exists (
    select null from crm_calltasklog log
    where log.task_id = t.id
    and log.status = 17
)
and t.status = 1 -- Успешная продажа

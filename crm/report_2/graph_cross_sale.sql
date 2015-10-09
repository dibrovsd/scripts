select
    'Создано задач' as title,
    count(1) as value
from ({{datasets.base.sql}}) t

union all

select
    'Продажа' as title,
    count(1) as value
from ({{datasets.base.sql}}) t
where t.status = 1 -- Успешная продажа


union all

select
    'Кросс-продажи' as title,
    count(1) as value
from ({{datasets.base.sql}}) t
where exists (
    select 1
)
and t.status = 1 -- Успешная продажа

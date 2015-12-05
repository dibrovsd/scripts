with

-- Отдан нотариусу
to_notarius as (
    select
        e.d_create,
        e.user_responsible_id as user_id,
        d.blank_type,
        e.document_id,
        e.event_id,
        row_number() over(partition by e.document_id order by e.d_create desc) as rn
    from docflow_documentevent1 e
    join docflow_document1 d on d.id = e.document_id
    where e.state_to_id = 2 -- на Выдан продавцу
    and exists (
        select null from base_user u
        inner join base_channel ch on ch.id = u.channel_id
        where u.id = e.user_responsible_id
        and ch.root_id = 15 -- передан Нотариусу
    )
),

-- Передан Выдан продавцу > Распределение
-- по этому документу до этого была передача на нотариуса (как в логике выше)
from_user as (
    select
        e.d_create,
        e1.user_id,
        d.blank_type,
        e.document_id,
        e.event_id
    from docflow_documentevent1 e
    join docflow_document1 d on d.id = e.document_id
    -- Предыдущая передача на нотариуса
    join to_notarius e1 on e1.document_id = e.document_id and e1.d_create < e.d_create and e1.rn = 1
    where e.state_from_id = 2 -- Выдан продавцу
    and e.state_to_id = 1 -- Распределение
),

data as (
    select
        'to' as m,
        d_create::date as d_create,
        user_id,
        blank_type,
        document_id,
        event_id
    from to_notarius
    where d_create between [[env.period.0]] and [[env.period.1]]

    union all

    select
        'from' as m,
        d_create::date as d_create,
        user_id,
        blank_type,
        document_id,
        event_id
    from from_user
    where d_create between [[env.period.0]] and [[env.period.1]]
)

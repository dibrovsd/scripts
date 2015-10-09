select
    coalesce(l.title, t.status::varchar) as title,
    l.color,
    count(1) as value
from ({{datasets.base.sql}}) t
left join reports.calltask_status l on l.id = t.status
group by coalesce(l.title, t.status::varchar), l.color

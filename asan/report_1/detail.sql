with base as (
   select
        t.d_create at time zone 'Asia/Baku' as d_create,
        t.path,
        substring(t.path from '\d+') as num
    from lib_logaccess t
    where t.d_create between [[env.d_report]] and [[env.d_report]] + interval '1 day - 1 second'
      and t.user_id = [[get.user_id]]::integer
)

select
    to_char(base.d_create, 'dd.mm.yyyy hh:mi:ss') as "Дата посещения",
    base.path as "Адрес",
    t1.title as "Отчет"
from base
left join reports_report t1 on base.path like '/reports/%' and t1.id::varchar = base.num
order by base.d_create
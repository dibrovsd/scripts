-- Лог посещения системы

with base as (
   select
        t.d_create at time zone 'Asia/Baku' as d_create,
        t.path,
        replace(replace(t.path, '/reports/', ''), '/', '') as report_id
    from lib_logaccess t
    where t.d_create >= to_date('01.01.2015', 'dd.mm.yyyy')
      and t.user_id = 67 -- asan_authority
      and t.path like '/reports/%'
)

select
    to_char(base.d_create, 'dd.mm.yyyy hh:mi:ss') as "Дата посещения",
    base.path as "Адрес",
    t1.title as "Отчет"
from base
left join reports_report t1 on t1.id::varchar = base.report_id
order by base.d_create
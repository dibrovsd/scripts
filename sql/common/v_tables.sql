create view v_tables as
select t.table_schema, 
    t.table_name, 
    t2.reltuples as num_rows 
from information_schema.tables t 
join pg_namespace t1 on t1.nspname = t.table_schema
join pg_class t2 on t2.relname = t.table_name
    and t2.relnamespace = t1.oid
where t.table_schema = 'public'
order by num_rows desc
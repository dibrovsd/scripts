select 'alter table '||t.table_name||' DROP CONSTRAINT '||t."constraint_name"|| ';' from information_schema.table_constraints t
where t.table_schema = 'public'
and t.constraint_type = 'FOREIGN KEY';
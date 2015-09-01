create view v_table_columns as
 select t.table_schema, 
    t.table_name, 
    t.column_name, 
    t.data_type, 
    t.is_nullable 
from information_schema.columns as t
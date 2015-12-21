with base as (
    select
        t.table_name,
        string_agg(t.column_name, ', ') as cols
    from v_table_columns t
    where t.table_schema = 'public'
    and t.table_name like 'df_integration%'
    group by t.table_name
)

select 'delete from public.'||table_name||'; insert into public.'||table_name||' ('|| cols ||') select '|| cols ||' from asan.'||table_name||';' from base

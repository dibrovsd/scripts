insert into docflow_fieldaccess1 (field_id, state_id, role_id, tag_id, responsible, can_edit, d_change)
with res as (
    select 
        t.id as field_id,
        t2.id as state_id, 
        t1.role_id,
        null as tag_id, 
        t1.responsible, 
        t1.can_edit,
        sysdate as d_change
    from docflow_field1 t
    cross join (
        select 12 as role_id, 1 as responsible, 0 as can_edit from dual union all
        select 20 as role_id, 1 as responsible, 0 as can_edit from dual union all
        select 21 as role_id, 1 as responsible, 0 as can_edit from dual union all
        select 24 as role_id, 1 as responsible, 0 as can_edit from dual union all
        select 25 as role_id, 1 as responsible, 0 as can_edit from dual union all
        select 12 as role_id, 0 as responsible, 0 as can_edit from dual union all
        select 20 as role_id, 0 as responsible, 0 as can_edit from dual union all
        select 21 as role_id, 0 as responsible, 0 as can_edit from dual union all
        select 24 as role_id, 0 as responsible, 0 as can_edit from dual union all
        select 25 as role_id, 0 as responsible, 0 as can_edit from dual
    ) t1
    join docflow_state1 t2 on 1 = 1
    where t.id in (59)
) 
select field_id, state_id, role_id, tag_id, responsible, can_edit, d_change
from res
where (field_id, state_id, role_id, tag_id, responsible) not in (
    select field_id, state_id, role_id, tag_id, responsible
    from docflow_fieldaccess1
)
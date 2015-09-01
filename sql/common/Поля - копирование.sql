insert into docflow_fieldaccess1
    (field_id, state_id, role_id, tag_id, responsible, can_edit, d_change)

select t.field_id, 
    t.state_id, 
    t.role_id, 
    t.tag_id, 
    t.responsible, 
    t.can_edit, 
    current_timestamp d_change
from docflow_fieldaccess1 t
where t.field_id = XXX
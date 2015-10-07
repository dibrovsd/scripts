-- ="select "&I2&" as field_id, "&J2&" as state_id, "&K2&" as role_id union all"
insert into docflow_fieldaccess1 (field_id, state_id, role_id, tag_id, responsible, can_edit, d_change)
with data as (
    

)
select 
    field_id,
    state_id::integer as state_id,
    role_id,
    null as tag_id,
    true as responsible,
    true as can_edit,
    current_timestamp as d_change
from data
select
	t.d_create,
	t1.last_name,
	t.field_name,
	t.field_value_verbose
from docflow_documentlog1 t
inner join base_user t1 on t1.id = t.user_id
where t.document_id = 1904
--and t.d_create >= current_date
and t.field_name = 'direction_stoa'
order by t.d_create

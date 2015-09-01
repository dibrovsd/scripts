update docflow_document1
    set repair_date = repair_date_src
from (
    select
    	t.id as document_id,
    	(to_timestamp(substr(l.field_value_verbose, 1, 19), 'yyyy-mm-dd hh24:mi:ss') - interval '1 hour') at time zone 'Europe/Moscow' as repair_date_src
    from docflow_document1 t
    join (
    	select
    		l.document_id,
    		l.field_value_verbose,
    		row_number() over(partition by l.document_id order by d_create desc) as rn
    	from docflow_documentlog1 l
    	where l.field_name = 'repair_date'
    ) l on l.document_id = t.id and l.rn = 1
    where t.repair_date is null
    and l.field_value_verbose != 'None'
) t
where document_id = id

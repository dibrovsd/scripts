/**
* Исправляет ссылку на последний переход
*/

update docflow_document1
	set last_event_id = last_event_src
from (
	select
		t2.id as id_src,
		t2.last_event_id_new as last_event_src
	from (
	select
		t.id,
		t.last_event_id,
		(
			select max(t1.id)
			from docflow_documentevent1 t1
			where t1.document_id = t.id
		) as last_event_id_new
	from docflow_document1 t
	) t2
	where t2.last_event_id != t2.last_event_id_new
) t3
where id = id_src

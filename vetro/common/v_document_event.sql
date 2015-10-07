drop view reports.v_document_event;


create or replace view reports.v_document_event as
select de.document_id,
       de.d_create,
       e.state_to_id,
       e.state_from_id
from docflow_documentevent1 de
inner join docflow_event1 e on e.id = de.event_id
where exists (
    select null from docflow_document1 d
    where d.id = de.document_id
      and d.deleted = false
)

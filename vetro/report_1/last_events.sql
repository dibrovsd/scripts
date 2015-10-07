create view reports.rep_1_last_events as

select t.*
from (
    select
        de.document_id,
        de.d_create,
        e.state_to_id,
        e.state_from_id,
        row_number() over(partition by de.document_id, e.state_to_id order by de.d_create desc) as rn
    from docflow_documentevent1 de
    inner join docflow_event1 e on e.id = de.event_id
    where e.state_to_id in (11, 12)
) t
where t.rn = 1

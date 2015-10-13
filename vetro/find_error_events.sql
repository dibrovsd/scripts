with base as (
    select
        de.id,
        (
            select max(de_prev.id)
            from docflow_documentevent1 de_prev
            where de_prev.document_id = de.document_id
            and de_prev.id < de.id
            and de_prev.processing_state = 1 -- принятые
        ) as de_prev,
        de.state_from_id,
        de.event_id
    from docflow_documentevent1 de
    where exists (
        select null from docflow_document1 d
        where d.id = de.document_id
        and d.deleted = false
    )
)


select
    base.id,
    ade_prev.document_id,
    ade_prev.state_to_id
from base
-- Предыдущий
join docflow_documentevent1 ade_prev on ade_prev.id = de_prev
where base.state_from_id != ade_prev.state_to_id
order by 1, 2;


update docflow_documentevent1
    set state_from_id = from_src,
    state_to_id = to_src
from (
    select e_old.id as id_src,
        e_old.state_from_id as from_src,
        e_old.state_to_id as to_src
    from old.docflow_event1 e_old
    where e_old.id = 53
) src
where event_id = id_src

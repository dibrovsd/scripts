
update issues 
set due_date = closed_at
from (
    select 
        t.journalized_id as issue_id,
        min(created_on) as closed_at
    from journals t
    join journal_details t1 on t1.journal_id = t.id 
                    and t1.prop_key = 'status_id' 
                    and t1.value = '5'
    where t.journalized_type = 'Issue'
    group by t.journalized_id
) t 
where id = issue_id 
    and status_id = 5
    and due_date is null;

update issues 
set done_ratio = 100
where status_id = 5
    and done_ratio != 100
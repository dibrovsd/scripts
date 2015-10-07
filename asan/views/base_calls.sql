drop view reports.base_calls;

create or replace view reports.base_calls as
select 
    u.id as user_id,
    t.calldate at time zone 'Asia/Baku' as calldate,
    t.disposition,
    t.des_md5,
    round(t.duration::numeric / 60, 2) as duration
from base_asteriskcall t
inner join base_user u on u.asterisk_ext::varchar = t.src

{{datasets.src.sql}}

select
    d.user_id,
    u.last_name || ' ' || u.first_name as user,
    {% for row in datasets.dates.data %}
        count(case when d.m = 'to' and d.d_create = to_date('{{row.d_start}}', 'dd.mm.yyyy') then 1 end) as "{{row.d_start}}_to",
        count(case when d.m = 'from' and d.d_create = to_date('{{row.d_start}}', 'dd.mm.yyyy') then 1 end) as "{{row.d_start}}_from",
    {% endfor %}
    null as dummy
from data d
join base_user u on u.id = d.user_id
where d.blank_type = 2 -- S7
group by user_id, u.last_name || ' ' || u.first_name

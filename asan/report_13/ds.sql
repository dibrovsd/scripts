select
    u.last_name || ' ' || u.first_name as "Оператор",
    t.calldate as "Дата звонка",
    t.disposition as "Статус звонка",
    t.duration as "Продолжительность (мин)"
from reports.base_calls t
inner join base_user u on u.id = t.user_id
where des_md5 = md5([[env.phone]])
order by t.calldate
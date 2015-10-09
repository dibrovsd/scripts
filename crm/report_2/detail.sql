
select
    t.id,
    t.d_create as "Создана",
    task_type.title as "Тип задачи",
    st.title as "Статус",
    u.last_name || ' ' || u.first_name as "Ответственный"
from ({{datasets.base.sql}}) t
inner join reports.task_type on task_type.name = t.task_type
inner join reports.calltask_status st on st.id = t.status
inner join base_user u on u.id = t.responsible_id


{% if get.dt == 'status' %}
    where t.status = [[get.status]]::integer

{% elif get.dt == 'other_regect' %}
    where t.status in (6, 7, 9, 10, 13, 15, 18)

{% elif get.dt == 'ratio_sale' %}
    where t.status = 1

{% endif %}

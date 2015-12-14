select
    t.d_create as "Дата изменения",
    t1.title as "Параметр",
    t.field_value_verbose as "Значение",
    u.last_name || ' '|| u.first_name as "Оператор"
from docflow_documentlog1 t
join docflow_field1 t1 on t1.name = t.field_name
join base_user u on u.id = t.user_id
where t.document_id = [[get.document_id]]::integer
{% if env.period %}
    and t.d_create between [[env.period.0]] and [[env.period.1]]
{% endif %}
order by 1, 2
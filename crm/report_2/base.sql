select
    t.id,
	t.status,
	t.responsible_id,
    t.task_type,
    t.d_create
from crm_calltask t
where t.d_create between [[env.period.0]] and [[env.period.1]]
and t.task_type in ('renew_osago', 'renew_realty')

{% if env.task_type %}
    and t.task_type in ('{{env.task_type|join:"','"}}')
{% endif %}

{% if env.responsible != '0' %}
    and t.responsible_id = [[env.responsible]]::integer
{% endif %}

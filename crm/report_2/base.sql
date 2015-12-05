select
    t.id,
	t.status,
	t.responsible_id,
    t.task_type,
    t.d_create
from crm_calltask t
where t.d_create between [[env.period.0]] and [[env.period.1]]

{% if env.expire_period %}
    and t.d_expire between [[env.expire_period.0]] and [[env.expire_period.1]]
{% endif %}

{% if env.task_type %}
    and t.task_type in ('{{env.task_type|join:"','"}}')
{% endif %}

{% if env.responsible != '0' %}
    and t.responsible_id = [[env.responsible]]::integer
{% endif %}

{% if env.client_with_fin %}
    and exists (
        select null from crm_client cl
        where cl.id = t.client_id
          and exists (
              select null from crm_clientperson person
              where person.client_id = t.client_id
                and person.pin is not null
          )
    )
{% endif %}
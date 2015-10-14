select
    u.id,
    u.last_name || ' ' || u.first_name as title
from base_user u
where
{% if env.users %}
     u.id in ({{env.users|join:","}})
{% else %}
    1 = 0
{% endif %}

order by title

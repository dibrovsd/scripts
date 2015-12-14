select
    t.id,
    t.direction_num,
    t.city,
    t.stoa
from reports.v_document t
where 1 = 1

{% if env.document_id %}
    and t.id = [[env.document_id]]::integer

{% elif env.direction_num %}
    and t.direction_num = [[env.direction_num]]

{% else %}
    and 1 = 0

{% endif %}

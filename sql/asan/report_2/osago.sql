select
    to_char(t.d_issue, 'yyyy-mm-dd') as дата_продажи,
    count(1) as колво,
    sum(t.s_premium) as премия,
    sum(t.s_comission) as комиссия
from reports.base_osago t
where 1 = 1
{% if 'call_centre' in user_params.seller_territory or env.seller_territory == 'call_centre' %}
    and t.seller_territory_id = 9
{% elif env.seller_territory == 'asan' %}
    and t.seller_territory_id != 9
{% endif %}
{% if env.seller %}
    and t.seller_id = {{env.seller}}
{% endif %}
group by to_char(t.d_issue, 'yyyy-mm-dd')
order by 1
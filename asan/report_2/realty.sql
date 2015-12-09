select
    to_char(t.d_issue, 'yyyy-mm-dd') as дата_продажи,
    count(1) as колво,
    sum(t.s_premium) as премия,
    sum(t.s_comission) as комиссия
from reports.base_realty t
where 1 = 1
{% if 'call_centre' in user_params.channel %}
    and t.channel_root_id = 9

{% elif env.channel %}
    and t.channel_root_id = [[env.channel]]::integer

{% endif %}
{% if env.seller %}
    and t.seller_id = {{env.seller}}
{% endif %}
group by to_char(t.d_issue, 'yyyy-mm-dd')
order by 1
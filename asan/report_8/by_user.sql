with params as (
    select
           {% if not env.period_type or env.period_type == 'month' %}
                date_trunc('month', current_date) as d_start,
                current_date + interval '1 day - 1 second' as d_end

           {% else %}
               [[env.period.0]] as d_start,
               [[env.period.1]] as d_end

           {% endif %}

           -- to_date('01.05.2015', 'dd.mm.yyyy') as d_start,
           -- to_date('01.06.2015', 'dd.mm.yyyy') + interval '1 day - 1 second' as d_end
)

select t1.last_name ||' '|| t1.first_name as "Продавец",
    count(1) as "Итого продаж",
    sum(t.s_comission) as "Комиссия за вычетом скидок",
    sum(t.s_discount) as "Сумма скидок"
from reports.base_sales t
cross join params
inner join base_user t1 on t1.id = t.seller_id
where t.d_issue between params.d_start and params.d_end
    {% if env.channel %}
        and t.channel_root_id = [[env.channel]]::integer
    {% endif %}

    {% if 'call_center' in user_params.territory_only %}
        and t.channel_root_id = 9
    {% elif 'asan' in user_params.territory_only %}
        and t.channel_root_id = 7
    {% endif %}
group by t1.last_name ||' '|| t1.first_name

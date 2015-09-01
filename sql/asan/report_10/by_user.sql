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
),

-- Звонки
calls as (
    select
        t.user_id,
        count(1) as cnt,
        count(distinct case when t.disposition = 'ANSWERED' then t.des_md5 end) as cnt_answer
    from reports.base_calls t
    cross join params
    where t.calldate between params.d_start and params.d_end
    group by t.user_id
),

-- Продажи
sales as (
    select
        t.seller_id,
        count(1) as cnt
    from reports.base_sales t
    cross join params
    where t.d_issue between params.d_start and params.d_end
      and seller_territory_id = 9
    group by t.seller_id
)

select
    u.last_name ||' '|| u.first_name as "Продавец",
    calls.cnt as "Звонков",
    calls.cnt_answer as "Отвечено звонков",
    round(sales.cnt::numeric / calls.cnt_answer * 100, 2) || '%' as "Проникновение"
from calls
inner join base_user u on u.id = calls.user_id
left join sales on sales.seller_id = calls.user_id

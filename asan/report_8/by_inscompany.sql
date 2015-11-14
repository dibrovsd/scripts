with params as (
    select
           {% if not env.period_type or env.period_type == 'month' %}
                date_trunc('month', current_date) as d_start,
                current_date + interval '1 day - 1 second' as d_end

           {% else %}
               [[env.period.0]] as d_start,
               [[env.period.1]] as d_end

           {% endif %}

        --    to_date('01.01.2015', 'dd.mm.yyyy') as d_start,
        --    to_date('01.05.2015', 'dd.mm.yyyy') - 1 as d_end
),

sales as (
    select
        s.seller_id,
        s.inscompany_id,
        sum(s.s_premium) as s_premium,
        count(1) as cnt
    from reports.base_sales s
    cross join params
    where s.d_issue between params.d_start and params.d_end
        {% if env.seller_territory == 'call_centre' %}
            and s.seller_territory_id = 9
        {% elif env.seller_territory == 'asan' %}
            and s.seller_territory_id != 9
        {% endif %}

        {% if 'call_center' in user_params.territory_only %}
            and s.seller_territory_id = 9
        {% elif 'asan' in user_params.territory_only %}
            and s.seller_territory_id != 9
        {% endif %}
    group by s.seller_id, s.inscompany_id
),

with_ratio as (
    select s.seller_id,
        s.inscompany_id,
        s.s_premium,
        s.cnt / sum(s.cnt) over(partition by s.seller_id) * 100 as cnt_ratio
    from sales s
)

select
    u.last_name ||' '|| u.first_name as "Продавец",
    {% for ins in datasets.inscompanys.data %}
        max(case
            when s.inscompany_id = {{ins.id}}
            then s.s_premium ||' ('|| round(cnt_ratio, 1) ||')'
        end) as "{{ins.title}}",
    {% endfor %}
    null as dummy
from with_ratio s
join base_user u on u.id = s.seller_id
group by u.last_name ||' '|| u.first_name

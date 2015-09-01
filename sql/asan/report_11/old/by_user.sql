with params as (
    select
        {% if get.period == 'день' %}
            current_date as d_start

        {% else %}
            date_trunc('month', current_date) as d_start

        {% endif %}

        -- current_date as d_start
),

users as (
    select
        u.id as user_id,
        u.last_name ||' '|| u.first_name as user
    from base_user u
    where u.territory_id = 9
),

sales as (
    select
        s.seller_id,
        count(1) as cnt
    from reports.base_sales s
    cross join params
    where s.seller_territory_id = 9
      and s.d_issue >= params.d_start
    group by s.seller_id
)

select
    users.user,
    sales.cnt
from sales
inner join users on sales.seller_id = users.user_id
order by sales.cnt desc

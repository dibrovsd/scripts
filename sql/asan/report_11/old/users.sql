with params as (
    select
        {% if get.period == 'день' %}
            current_date as d_start
        {% else %}
            date_trunc('month', current_date) as d_start
        {% endif %}

        -- current_date as d_start
),


sales as (
    select
        s.seller_id,
        count(1) as cnt
    from reports.base_sales s
    cross join params
    where s.seller_territory_id = 9
        and s.d_issue > params.d_start
    group by s.seller_id
)

select
	u.id,
	u.last_name ||' '|| u.first_name as title
from base_user u
inner join sales on sales.seller_id = u.id
where u.territory_id = 9
order by sales.cnt desc

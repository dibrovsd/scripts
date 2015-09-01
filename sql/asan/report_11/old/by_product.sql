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
        u.asterisk_ext::varchar as asterisk_ext,
        u.last_name,
        u.first_name
    from base_user u
    where u.asterisk_ext is not null
),

sales as (
    select s.seller_id, s.product
    from reports.base_sales s
    cross join params
    where s.seller_territory_id = 9
        and s.d_issue >= params.d_start
),

sales_gr as (
    select
        seller_id,
        count(1) as cnt,
        count(case when product = 'ОСАГО' then 1 end) as cnt_osago,
        count(case when product = 'Недвижимость' then 1 end) as cnt_realty,
        count(case when product = 'ВЗР' then 1 end) as cnt_travel,
        count(case when product = 'Уверенный водитель' then 1 end) as cnt_confident_driver,
        count(case when product = 'Просто КАСКО' then 1 end) as cnt_simple_kasko,
        count(case when product = 'Пятерочка' then 1 end) as cnt_raider_five,
        count(case when product = 'ОСАГО+' then 1 end) as cnt_raider_osago_plus,
        count(case when product = 'Супер КАСКО' then 1 end) as cnt_raider_super_kasko
    from sales
    group by seller_id
)

select
    row_number() over(order by cnt desc nulls last) as "№",
    u.last_name ||' '|| u.first_name as "Оператор",
    cnt as "Итого",
    cnt_osago as "ОСАГО",
    cnt_realty as "Недвижимость",
    cnt_travel as "ВЗР",
    cnt_confident_driver as "Уверенный водитель",
    cnt_simple_kasko as "Просто КАСКО",
    cnt_raider_five as "Пятерочка",
    cnt_raider_osago_plus as "ОСАГО+",
    cnt_raider_super_kasko as "Супер КАСКО"
from users u
left join sales_gr on sales_gr.seller_id = u.user_id
order by cnt desc nulls last

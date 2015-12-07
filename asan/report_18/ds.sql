with params as (
    -- select current_date - 30 as d_from, current_date as d_to
    select [[env.period.0]] as d_from, [[env.period.1]] as d_to
),

sales as (
    select s.*
    from reports.base_sales s
    cross join params
    where s.d_issue between params.d_from and params.d_to
      and s.seller_territory_id != 9
),

asan_actions as (
    select a.*
    from base_asanactions a
    cross join params
    where a.d_action between params.d_from and params.d_to
),

sales_gr as (
    select
        sales.seller_territory_id as territory_id,
        sales.seller_id,
        count(case when sales.product = 'ОСАГО' then 1 end) as cnt_osago,
        count(case when sales.product = 'Недвижимость' then 1 end) as cnt_realty,
        count(distinct sales.contractor_id) as cnt_contractors
    from sales
    group by sales.seller_territory_id, sales.seller_id
),

asan_actions_gr as (
    select
        a.asan,
        sum(a.served_users) as cnt_all,
        sum(case
                when a.action in ('K  -  Notariat fəaliyyəti', 'L  -  Daşınmaz əmlakla bağlı əməliyyatların qeydiyyatı')
                then a.served_users
        end) as cnt_insurance
    from asan_actions a
    group by a.asan
),

-- Связываем асаны наши и их
asan_map as (
    select asan_map.*
    from (
        select '1_sayli' as asan, 1 as territory, 'ASAN 1' as title union all
        select '2_sayli' as asan, 2 as territory, 'ASAN 2' as title union all
        select '3_sayli' as asan, 3 as territory, 'ASAN 3' as title union all
        select '4_sayli' as asan, 4 as territory, 'ASAN 4' as title union all
        -- select '5_sayli' as asan, 0 as territory union all
        select 'berde' as asan, 8 as territory, 'Berde' as title union all
        select 'gence' as asan, 6 as territory, 'Gence' as title union all
        select 'sabirabad' as asan, 7 as territory, 'Sabirabad' as title union all
        select 'sum' as asan, 5 as territory, 'Sumgait' as title
    ) asan_map
    {% if env.asan %}
    where territory in ({{env.asan|join:", "}})
    {% endif %}
)

select
    asan_map.title as "АСАН",
    u.last_name || ' ' || u.first_name as "Продавец",
    --
    sales_gr.cnt_osago as "Продаж ОСАГО",
    sales_gr.cnt_realty as "Продаж Недвижимости",
    --
    asan_actions_gr.cnt_all as "Операции все",
    asan_actions_gr.cnt_insurance as "Операции страховые",
    --
    round((f_division(sales_gr.cnt_osago, asan_actions_gr.cnt_insurance) * 100)::numeric, 2) as "Доля ОСАГО",
    round((f_division(sales_gr.cnt_realty, asan_actions_gr.cnt_insurance) * 100)::numeric, 2) as "Доля Недвижимости",
    round((f_division(sales_gr.cnt_contractors, asan_actions_gr.cnt_insurance) * 100)::numeric, 2) as "Доля продаж по услугам"
from asan_map
left join sales_gr on sales_gr.territory_id = asan_map.territory
left join asan_actions_gr on asan_actions_gr.asan = asan_map.asan
inner join base_user u on u.id = sales_gr.seller_id
order by 1,2

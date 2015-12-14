with params as (
    -- select current_date - 30 as d_from, current_date as d_to
    select [[env.period.0]] as d_from, [[env.period.1]] as d_to
),

sales as (
    select s.*
    from reports.base_sales s
    cross join params
    where s.d_issue between params.d_from and params.d_to
        {% if env.channel %}
            and s.channel_territory_id in ({{env.channel|join:", "}})
        {% endif %}
        and s.channel_root_id = 7 -- АСАН
),

asan_actions as (
    select a.*
    from base_asanactions a
    cross join params
    where a.d_action between params.d_from and params.d_to
),

sales_gr as (
    select
        sales.channel_territory_id as channel_id,
        sales.seller_id,
        count(case when sales.product = 'ОСАГО' then 1 end) as cnt_osago,
        count(case when sales.product = 'Недвижимость' then 1 end) as cnt_realty,
        count(distinct sales.contractor_id) as cnt_contractors
    from sales
    group by sales.channel_territory_id, sales.seller_id
),

asan_actions_gr as (
    select
        a.asan,
        sum(a.served_users) as cnt_all,
        sum(case
                when a.action in ('K  -  Notariat fəaliyyəti', 'L  -  Daşınmaz əmlakla bağlı əməliyyatların qeydiyyatı')
                then a.served_users
        end) as cnt_realty_notar,
        sum(case when a.action = 'S  -  Sığorta' then a.served_users end) as cnt_sigorta
    from asan_actions a
    group by a.asan
),

-- Связываем асаны наши и их
asan_map as (
    select asan_map.*
    from (
        select '1_sayli' as asan, 1 as channel, 'ASAN 1' as title union all
        select '2_sayli' as asan, 2 as channel, 'ASAN 2' as title union all
        select '3_sayli' as asan, 3 as channel, 'ASAN 3' as title union all
        select '4_sayli' as asan, 4 as channel, 'ASAN 4' as title union all
        select '5_sayli' as asan, 68 as channel, 'ASAN 5' as title union all
        select 'berde' as asan, 8 as channel, 'Berde' as title union all
        select 'gence' as asan, 6 as channel, 'Gence' as title union all
        select 'sabirabad' as asan, 69 as channel, 'Sabirabad' as title union all
        select 'sum' as asan, 5 as channel, 'Sumgait' as title
    ) asan_map
    {% if env.channel %}
        where channel in ({{env.channel|join:", "}})
    {% endif %}
)

select
    asan_map.title as "АСАН",
    u.last_name || ' ' || u.first_name as "Продавец",
    --
    sales_gr.cnt_osago as "ОСАГО",
    sales_gr.cnt_realty as "Недвижимости",
    --
    asan_actions_gr.cnt_all as "Оказанные услуги",
    asan_actions_gr.cnt_realty_notar as "Услуги нотариата и имущества",
    --
    round((f_division(sales_gr.cnt_osago, asan_actions_gr.cnt_realty_notar) * 100)::numeric, 2) as "Доля ОСАГО",
    round((f_division(sales_gr.cnt_realty, asan_actions_gr.cnt_realty_notar) * 100)::numeric, 2) as "Доля Недвижимости",
    --
    asan_actions_gr.cnt_sigorta as "Оказанные услуги по страхованию",
    round((f_division(sales_gr.cnt_contractors, asan_actions_gr.cnt_sigorta) * 100)::numeric, 2) as "Доля продаж по страх. услугам"
from asan_map
left join sales_gr on sales_gr.channel_id = asan_map.channel
left join asan_actions_gr on asan_actions_gr.asan = asan_map.asan
inner join base_user u on u.id = sales_gr.seller_id
order by 1,2
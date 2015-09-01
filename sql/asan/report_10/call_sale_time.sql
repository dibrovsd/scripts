with params as (
    select
        {% if not env.period_type or env.period_type == 'month' %}
            date_trunc('month', current_date) as d_start,
            current_date + interval '1 day - 1 second' as d_end

        {% else %}
           [[env.period.0]] as d_start,
           [[env.period.1]] as d_end

        {% endif %}


       -- to_date('01.01.2014', 'dd.mm.yyyy') as d_start,
       -- current_date as d_end
),

sales as (
    select * from (
        -- ОСАГО
        select t.d_start, t.ins_phone, 'ОСАГО' as product, t.id, t.auto_number, t.d_issue
        from reports.base_osago t
        where t.seller_territory_id = 9
        -- Уверенный водитель
        union all
        select t.d_start, t.ins_phone, 'Уверенный водитель' as product, t.id, t.auto_number, t.d_issue
        from reports.base_confident_driver t
        where t.seller_territory_id = 9
        -- Просто КАСКО
        union all
        select t.d_start, t.ins_phone, 'Просто КАСКО' as product, t.id, t.auto_number, t.d_issue
        from reports.base_simple_kasko t
        where t.seller_territory_id = 9
    ) t
    cross join params
    where t.d_issue between params.d_start and params.d_end
),

calls as (
    select
        t.des_md5 as phone_md5,
        t.calldate at time zone 'Asia/Baku' as calldate,
        rank() over(partition by t.des_md5 order by t.calldate) as call_rank
    from base_asteriskcall t
    where exists (
            select null from base_user
            where base_user.asterisk_ext::varchar = t.src
        )
)

-- select
--     auto_contracts.vehicle_number,
--     calls.calldate,
--     auto_contracts.contract_to as d_contract_end
-- from calls
-- inner join sales on sales.ins_phone = calls.phone
-- inner join tmp_store.auto_contracts on auto_contracts.vehicle_number = sales.auto_number
-- where calls.call_rank = 1 -- Первый звонок этому страхователю
-- order by calls.calldate - auto_contracts.contract_to

select
    call_delay,
    count(1) as cnt
from (
    select
        round(EXTRACT(epoch FROM call_delay)::float / 60 / 60 / 24)::integer as call_delay
    from (
        select
            auto_contracts.contract_to - calls.calldate as call_delay
        from sales
        inner join calls on md5(sales.ins_phone) = calls.phone_md5
        inner join reports.auto_contracts on auto_contracts.vehicle_number = sales.auto_number
        where calls.call_rank = 1 -- Первый звонок этому страхователю
    ) t
) t
group by call_delay
order by call_delay

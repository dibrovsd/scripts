drop view reports.base_osago;
create or replace view reports.base_osago as

with t as (
    select
        t.id,
        t.d_start at time zone 'Asia/Baku' as d_start,
        t.d_create at time zone 'Asia/Baku' as d_create,
        t.d_issue at time zone 'Asia/Baku' as d_issue,
        t.auto_mark_id,
        t.ins_person_birthday,
        t2.title::int as auto_createyear,
        t.ins_person_pin,
        t.ins_person_gender,
        t.ins_phone,
        t.auto_number,
        t.s_premium_base,
        t.ins_person_lname ||' '|| t.ins_person_fname as ins_person,
        t.contractor_id,
        t.s7_id,
        t.bso_id,
        -- Суммы
        t.s_premium as s_premium_with_rider,
        coalesce(raiders.cost, 0) as s_premium_rider,
        coalesce(t.discount_percent::numeric / 100, 0) as discount_ratio,
        case
            when t.inscompany_id in (5,8,11,12) then 0.3
            else 0.25
        end as comission_percent,
        -- Доставка
        t.delivery_date,
        t.delivery_time_from,
        t.delivery_time_to,
        t.delivery_region,
        t.delivery_address,
        t.delivery_city_id,
        t.delivery_comments,
        --
        t.n_contract,
        t.inscompany_id,
        t.user_creator_id as seller_id,
        t1.territory_id as seller_territory_id
    from docflow_document2 t
    inner join base_user t1 on t1.id = t.user_creator_id
    left join docflow_createyear t2 on t2.id = t.auto_createyear_id
    -- Райдеры Атешгях вычитаем из премии, потому что нужна премия ОСАГО
    -- отдельной от райдеров (райдеры будут посчитаны отдельно)
    left join (
        select
            t.document2_id as document_id,
            sum(t3.cost) as cost
        from docflow_document2_product_options t
        inner join docflow_p2productoption t3 on t3.system_name = t.p2productoption_id
        where t3.system_name in ('ateshgah-superkasko', 'ateshgah-icbariplus', 'ateshgah-beshlik')
        group by t.document2_id
    ) raiders on raiders.document_id = t.id
                 and t.inscompany_id = 1
    where
        t.d_issue is not null
        and t.canceled = false
),

t1 as (
    select
        t.*,
        t.s_premium_with_rider - t.s_premium_rider as s_premium
    from t
),

t2 as (
    select
        t1.*,
        round(t1.s_premium::numeric * t1.discount_ratio, 2) as s_discount
    from t1
)

select
    t2.*,
    t2.s_premium - t2.s_discount as premium_with_discount,
    t2.s_premium * comission_percent - t2.s_discount as s_comission,
    t2.s_premium * comission_percent as s_comission_no_discount
from t2;

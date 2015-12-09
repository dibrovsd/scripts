drop view reports.base_realty;
create or replace view reports.base_realty as

with t as (
    select
        t.id,
        t.n_contract,
        t.bso_id,
        t.s7_id,
        -- Даты
        t.d_start at time zone 'Asia/Baku' as d_start,
        t.d_create at time zone 'Asia/Baku' as d_create,
        t.d_issue at time zone 'Asia/Baku' as d_issue,
        -- Страхователь
        t.ins_person_birthday,
        t.ins_person_pin,
        t.ins_person_gender,
        t.ins_phone,
        t.contractor_id,
        t.ins_person_lname ||' '|| t.ins_person_fname as ins_person,
        -- Суммы
        t.s_premium,
        coalesce(t.discount_percent, 0) as discount_percent,
        case
            when t.inscompany_id in (5,8,12) then 0.3
            else 0.25
        end as comission_rate,
        -- Канал продаж
        t.channel_root_id,
        t.channel_sub_id,
        t.channel_territory_id,
        -- Доставка
        t.delivery_date,
        t.delivery_time_from,
        t.delivery_time_to,
        t.delivery_region,
        t.delivery_address,
        t.delivery_city_id,
        t.delivery_comments,
        -- Системные id
        t.inscompany_id,
        t.user_creator_id as seller_id,
        t1.territory_id as seller_territory_id
    from docflow_document3 t
    inner join base_user t1 on t1.id = t.user_creator_id
    where
        t.d_issue is not null
        and t.canceled = false
),

t1 as (
    select t.*,
        round(t.s_premium::numeric * t.discount_percent / 100, 2) as s_discount
    from t
)

select
    t1.*,
    t1.s_premium - t1.s_discount as s_premium_with_discount,
    t1.s_premium * t1.comission_rate - t1.s_discount as s_comission,
    t1.s_premium * t1.comission_rate as s_comission_no_discount
from t1;

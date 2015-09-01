drop view reports.base_travel;
create or replace view reports.base_travel as

with t as (
    select
        t.id,
        t.d_rest_start at time zone 'Asia/Baku' as d_start,
        t.d_create at time zone 'Asia/Baku' as d_create,
        t.d_issue at time zone 'Asia/Baku' as d_issue,
        t.s_premium as s_premium,
        t.s7_id,
        case
            when t.inscompany_id in (5,8,12) then 0.3
            else 0.25
        end as comission_percent,
        t.n_contract,
        t.contractor_id,
        t.ins_person_lname ||' '|| t.ins_person_fname as ins_person,
        t.inscompany_id,
        t.ins_person_birthday,
        t.ins_person_pin,
        t.ins_person_gender,
        t.ins_phone,
        -- Доставка
        t.delivery_date,
        t.delivery_time_from,
        t.delivery_time_to,
        t.delivery_region,
        t.delivery_address,
        t.delivery_city_id,
        t.delivery_comments,
        --
        t.user_creator_id as seller_id,
        t1.territory_id as seller_territory_id
    from docflow_document4 t
    inner join base_user t1 on t1.id = t.user_creator_id
    where
        t.d_issue is not null
        and t.canceled = false
)

select
    t.*,
    t.s_premium * comission_percent as s_comission
from t;

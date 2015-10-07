drop view reports.base_medicine;
create or replace view reports.base_medicine as

select
    t.id,
    t.inscompany_id,
    t.ins_person_birthday,
    t.ins_person_pin,
    t.ins_person_gender,
    t.ins_person_lname ||' '|| t.ins_person_fname as ins_person,
    t.n_contract,
    t.ins_phone,
    t.delivery_city_id,
    t.contractor_id,
    t.s7_id,
    -- Доставка
    t.delivery_date,
    t.delivery_time_from,
    t.delivery_time_to,
    t.delivery_region,
    t.delivery_address,
    t.delivery_comments,
    --
    t.d_start at time zone 'Asia/Baku' as d_start,
    t.d_create at time zone 'Asia/Baku' as d_create,
    t.d_issue at time zone 'Asia/Baku' as d_issue,
    --
	t.s_premium,
    t.s_premium * 0.15 as s_comission,
    --
    t.user_creator_id as seller_id,
    u.territory_id as seller_territory_id
from docflow_document9 t
inner join base_user u on u.id = t.user_creator_id
where t.deleted = false
    and t.d_issue is not null
    and t.canceled = false;

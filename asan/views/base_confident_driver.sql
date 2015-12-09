drop view reports.base_confident_driver;

create or replace view reports.base_confident_driver as

select
    t.id,
    t.inscompany_id,
    t2.title::int as auto_createyear,
    t.auto_mark_id,
    t.ins_person_birthday,
    t.ins_person_pin,
    t.ins_person_gender,
    t.auto_number,
    t.ins_phone,
    t.ins_person_lname ||' '|| t.ins_person_fname as ins_person,
    t.n_contract,
    t.contractor_id,
    t.s7_id,
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
    --
    t.d_start at time zone 'Asia/Baku' as d_start,
    t.d_create at time zone 'Asia/Baku' as d_create,
    t.d_issue at time zone 'Asia/Baku' as d_issue,
    --
    t.s_premium,
    t.s_premium * 0.2 as s_comission,
    --
    t.user_creator_id as seller_id,
    t1.territory_id as seller_territory_id
from docflow_document11 t
inner join base_user t1 on t1.id = t.user_creator_id
left join docflow_createyear t2 on t2.id = t.auto_createyear_id
where
    t.d_issue is not null
    and t.canceled = false;

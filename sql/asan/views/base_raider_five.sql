drop view reports.base_raider_five;
create or replace view reports.base_raider_five as

select
    t1.id,
    t1.inscompany_id,
    t1.ins_person_birthday,
    t1.ins_person_pin,
    t1.ins_person_gender,
    t1.auto_mark_id,
    t1.auto_number,
    createyear.title::int as auto_createyear,
    t1.ins_person_lname ||' '|| t1.ins_person_fname as ins_person,
    t1.n_contract,
    t1.ins_phone,
    t1.delivery_city_id,
    t1.contractor_id,
    t1.bso_id,
    t1.s7_id,
    -- Доставка
    t1.delivery_date,
    t1.delivery_time_from,
    t1.delivery_time_to,
    t1.delivery_region,
    t1.delivery_address,
    t1.delivery_comments,
    --
    t1.d_start at time zone 'Asia/Baku' as d_start,
    t1.d_create at time zone 'Asia/Baku' as d_create,
    t1.d_issue at time zone 'Asia/Baku' as d_issue,
    --
    t3.cost as s_premium,
    3 as s_comission,
    --
    t1.user_creator_id as seller_id,
    t2.territory_id as seller_territory_id
from docflow_document2_product_options t
inner join docflow_document2 t1 on t1.id = t.document2_id
inner join base_user t2 on t2.id = t1.user_creator_id
inner join docflow_p2productoption t3 on t3.system_name = t.p2productoption_id
left join docflow_createyear createyear on createyear.id = t1.auto_createyear_id
where t.p2productoption_id = 'ateshgah-beshlik'
    and t1.d_issue is not null
    and t1.canceled = false;

select t.column_name
from information_schema.columns as t
where t.table_schema = 'old'
and t.table_name = 'docflow_document1';

insert into docflow_document1 (
    id,
    d_create,
    refresh_tags,
    deleted,
    d_change,
    auto_vin,
    auto_engine_num,
    auto_number,
    auto_createyear,
    auto_mileage,
    auto_sts_num,
    direction_num,
    direction_date,
    direction_limit,
    direction_deductible,
    inspection_date,
    repair_date,
    deductible_is_payed,
    preorder_date,
    preorder_sum,
    invoice_date,
    repaired,
    documents_sended,
    stoa_repair_confirm,
    client_contract_num,
    client_fio,
    client_contract_start,
    client_email,
    auto_body_type_id,
    auto_color_id,
    auto_mark_id,
    auto_model_id,
    direction_stoa_id,
    inscompany_id,
    -- last_event_id,
    user_creator_id,
    pay_date,
    pay_sum,
    deductible_s_payed,
    direction_internal_num,
    glass_type,
    glass_full_heating,
    glass_heated_wipers,
    glass_light_sensor,
    glass_light_shieldin,
    glass_toning,
    glass_window_vin,
    city_auto_host_id,
    glass_company_id,
    glass_company_other,
    d_send_stoa_email_preorder,
    preorder_agreed,
    glass_rain_sensor,
    inspection_required,
    glass_has_in_stock,
    inspection_date_real,
    preorder_agreed_requered,
    s_repair_glass,
    s_repair_work,
    preorder_date_confirm,
    repair_date_real,
    direction_get_date,
    repair_available,
    d_documents_send,
    client_reject
)
select
    id,
    d_create,
    refresh_tags,
    deleted,
    d_change,
    auto_vin,
    auto_engine_num,
    auto_number,
    auto_createyear,
    auto_mileage,
    auto_sts_num,
    direction_num,
    direction_date,
    direction_limit,
    direction_deductible,
    inspection_date,
    repair_date,
    deductible_is_payed,
    preorder_date,
    preorder_sum,
    invoice_date,
    repaired,
    documents_sended,
    stoa_repair_confirm,
    client_contract_num,
    client_fio,
    client_contract_start,
    client_email,
    auto_body_type_id,
    auto_color_id,
    auto_mark_id,
    auto_model_id,
    direction_stoa_id,
    inscompany_id,
    -- last_event_id,
    user_creator_id,
    pay_date,
    pay_sum,
    deductible_s_payed,
    direction_internal_num,
    glass_type,
    glass_full_heating,
    glass_heated_wipers,
    glass_light_sensor,
    glass_light_shieldin,
    glass_toning,
    glass_window_vin,
    city_auto_host_id,
    glass_company_id,
    glass_company_other,
    d_send_stoa_email_preorder,
    preorder_agreed,
    glass_rain_sensor,
    inspection_required,
    glass_has_in_stock,
    inspection_date_real,
    preorder_agreed_requered,
    s_repair_glass,
    s_repair_work,
    preorder_date_confirm,
    repair_date_real,
    direction_get_date,
    repair_available,
    d_documents_send,
    false as client_reject
from old.docflow_document1 t
where t.id = 480;

insert into docflow_documentevent1 (
    id,
    d_create,
    message,
    processing_state,
    d_processed,
    message_processed,
    d_change,
    document_id,
    event_id,
    user_creator_id,
    user_processed_id,
    user_responsible_id
)
select
    id,
    d_create,
    message,
    processing_state,
    d_processed,
    message_processed,
    d_change,
    document_id,
    event_id,
    user_creator_id,
    user_processed_id,
    user_responsible_id
from old.docflow_documentevent1 where document_id = 480;

update docflow_document1
    set last_event_id = 9015
where id = 480

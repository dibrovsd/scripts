-- -- Если требуется очистка
-- truncate table crm_inscontractauto cascade;
-- truncate table crm_client cascade;
-- truncate table crm_calltask cascade;


insert into crm_client (
    d_create, phone,
    last_name, first_name, middle_name,
    address, client_type, baza_id
)
select
    current_timestamp,
    phone,
    vehicle_owner,
    '-',
    '-',
    address,
    1 as client_type,
    baza_id
from tmp_to_load src
where not exists (
    select null from crm_client cl
    where cl.baza_id = src.baza_id
);


insert into crm_inscontractauto (
    d_create, client_id,
    vehicle_number, brand, model,
    n_body, n_engine, create_year,
    insurance_company, contract_number,
    d_start, d_end,
    baza_id
)
select
    current_timestamp,
    cl.id as client_id,
    src.vehicle_number,
    src.vehicle_brand,
    src.vehicle_model,
    src.n_body,
    src.n_engine,
    case when length(src.create_year) = 4 then src.create_year::int end,
    src.insurance_company,
    src.contract_number,
    src.contract_from,
    src.contract_to,
    src.baza_id
from tmp_to_load src
inner join crm_client cl on cl.baza_id = src.baza_id
where not exists (
    select null from crm_inscontractauto auto
    where auto.baza_id = src.baza_id
);

drop table tmp_to_load;

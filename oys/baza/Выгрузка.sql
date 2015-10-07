drop table tmp_to_load;

create table tmp_to_load as
select 
    row_number() over() as client_id,
    t2.vehicle_number,
    t2.vehicle_brand,
    t2.vehicle_model,
    t.vladel as vehicle_owner,
    t.adr as address,
    t.sened as phone,
    t2.contract_number,
    t2.insurance_company,
    t2.contract_from,
    t2.contract_to,
    t.nbody as n_body,
    t.ndvig as n_engine,
    t.gvip as create_year,
    t."Id" as baza_id,
    t2.id as autocontract_id
from ( -- Берем последнего водителя по рег. номеру
    select
        t.*, 
        max(t."Id") over(partition by t.nomznak) as id_max 
    from baza t
) t
-- Из договоров страхования берем последний
join (
    select t2.*,
        row_number() over(partition by t2.vehicle_number order by t2.contract_to desc) as n_row
    from isbp_autocontract t2
) t2 on t2.vehicle_number = t.nomznak
        and t2.n_row = 1
where t."Id" = t.id_max
    -- Последний водитель имеет контактный телефон
    and exists (
        select null from v_baza t1
        where t1."Id" = t."Id"
    )

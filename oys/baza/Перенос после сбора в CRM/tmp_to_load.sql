drop table tmp_to_load;


create table tmp_to_load as
select row_number() over() as client_id,
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
from (
    -- Записи, которые насобирали за последние 2 недели
    select t2.*,
        row_number() over(partition by t2.vehicle_number order by t2.contract_to desc) as n_row
     from isbp_autocontract t2
    where t2.last_updated between current_date - 14 and current_date
) t2
join (
    select t.*,
        max(t."Id") over(partition by t.nomznak) as id_max
    from baza t
) t on t.nomznak = t2.vehicle_number
       and t."Id" = t.id_max
where t2.n_row = 1;

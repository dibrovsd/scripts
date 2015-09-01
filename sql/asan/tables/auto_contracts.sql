drop table imp.auto_contracts;

create table imp.auto_contracts as
select
	auto.vehicle_number,
	-- auto.brand as vehicle_brand,
	-- auto.model as vehicle_model,
	-- cl.last_name ||' '|| cl.first_name ||' '|| cl.middle_name as vehicle_owner,
	-- cl.address,
	-- cl.phone,
	-- auto.insurance_company,
	-- auto.d_start as contract_from,
	auto.d_end as contract_to
from crm_inscontractauto auto
inner join crm_client cl on cl.id = auto.client_id;

/*
Переносим ее в отчеты
pg_dump \
--format=c \
--host=localhost \
--username=django \
--table=auto_contracts \
crm > auto_contracts.bk

pg_restore \
--dbname=asan \
--format=c \
--host=localhost \
--username=django \
< auto_contracts.bk
*/

create table reports.auto_contracts as select * from public.auto_contracts;
create index reports.auto_contracts_idx1 on reports.auto_contracts (vehicle_number);
drop table public.auto_contracts;

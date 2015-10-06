/**
* Данные для сверки с CRM
*/

create table tmp_crm_copy as

select
	t.ins_person_pin,
	t.n_contract,
	'auto' as product
from docflow_document2 t
where t.canceled = false
and t.n_contract is not null

union ALL

select
	t.ins_person_pin,
	t.n_contract,
	'realty' as product
from docflow_document3 t
where t.canceled = false
and t.n_contract is not null

union ALL

select
	t.ins_person_pin,
	t.n_contract,
	'travel' as product
from docflow_document4 t
where t.canceled = false
and t.n_contract is not null

/*

pg_dump --format=c --host=localhost --username=django --table=tmp_crm_copy asan > ~/tmp_crm_copy.bk
pg_restore --dbname=crm --format=c --host=localhost --username=django < ~/tmp_crm_copy.bk

*/

select t.* from tmp_crm_copy t
where not exists (
	select null from crm_clientperson p
	where p.pin = t.ins_person_pin
)

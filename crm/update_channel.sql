create table tmp.contract_channel as
select
    s.project_id,
    s.id as document_id,
    s.n_contract,
    coalesce(ch1.title, '') || ' > ' || coalesce(ch2.title, '') || ' > ' || coalesce(ch3.title, '') as title
from reports.base_sales s
left join base_channel ch3 on ch3.id = s.channel_territory_id
left join base_channel ch2 on ch2.id = s.channel_sub_id
left join base_channel ch1 on ch1.id = s.channel_root_id;

/*
pg_dump --format=c --host=localhost --username=django --table=tmp.contract_channel asan > contract_channel.bk
pg_restore --dbname=crm --format=c --single-transaction --host=localhost --username=django < contract_channel.bk
*/

update crm_inscontractauto
set channel_verbose = channel_verbose_src
from (
    select t.id as id_src,
           t1.title as channel_verbose_src
    from crm_inscontractauto t
    join tmp.contract_channel t1 on t1.n_contract = t.contract_number
) src where id_src = id
and channel_verbose is null;
drop view reports.base_sales;

create or replace view reports.base_sales as

select t.id,
       t.seller_id,
       t.inscompany_id,
       'ВЗР' as product,
       t.s7_id,
       t.s_premium,
       t.d_issue,
       t.s_comission,
       t.ins_person,
       t.ins_phone,
       t.n_contract,
       t.contractor_id,
       4 as project_id
from reports.base_travel t

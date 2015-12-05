-- Отчет по пролонгации ОСАГО
-- с каких СК и на какие перешел человек

/* Выгрузить из АСАН */
create table tmp.base_osago as
select
    t.*,
    u.last_name || u.first_name as seller,
    i.title as inscompany
from reports.base_osago t
left join base_users u on u.id = t.seller_id
left join docflow_inscompany i on i.id = t.inscompany_id;

/* Закинуть в CRM */

/* К договорам в асане найти предыдущие договоры по CRM */
select
    t.n_contract,
    t.d_issue,
    t.inscompany,
    t.seller,
    t.s_premium,
    -- Предыдущий
    cnt.insurance_company,
    cnt.contract_number,
    cnt.d_end,
    cnt.channel
from tmp.base_osago t
-- Ищем предыдущий договор по CRM
left join crm_auto a on a.vehicle_number = t.auto_number and a.src_info = 'sale'
left join (
    select cnt.*, row_number() over(partition by cnt.auto_id order by cnt.d_end desc) as rn
    from crm_inscontractauto cnt
    where cnt.d_end < to_date('01.01.2015', 'dd.mm.yyyy')
) cnt on cnt.auto_id = a.id
      and cnt.rn = 1
where t.d_issue between to_date('01.10.2015', 'dd.mm.yyyy') and to_date('30.12.2015', 'dd.mm.yyyy');


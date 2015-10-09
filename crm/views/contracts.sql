create view reports.inscontract as

select
    c.contract_type as product,
    c.insurance_company,
    c.contract_number,
    c.d_start,
    c.d_end,
    c.channel,
    c.document_id
from crm_inscontractauto c

union all

select
    'realty' as product,
    c.insurance_company,
    c.contract_number,
    c.d_start,
    c.d_end,
    c.channel,
    c.document_id
from crm_inscontractrealty c

union all

select
    'travel' as product,
    c.insurance_company,
    c.contract_number,
    c.d_start,
    c.d_end,
    c.channel,
    c.document_id
from crm_inscontracttravel c;

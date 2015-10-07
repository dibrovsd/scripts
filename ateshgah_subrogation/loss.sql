-- по документам
select 
    t."Application_number" as Номер_убытка,
    t."Create_date" as Создан,
    -- Документы страхователя
    t1."Receive_date" as Дата_получения_документа,
    f_az2ru(t2."Decument_desc") as Документ
from claims t
-- Документы страхователя
left join document_link t1 on t1."Claim_guid" = t."Claim_guid"
left join documents t2 on t2."Document_oid" = t1.document_oid
limit 20

-- по убыткам
select
    t."Application_number" as номер_убытка,
    t."Create_date" as дата_регистрации,
    t2.recieve_claim as дата_заявления,
    t2.Receive_last as дата_последнего_документа
from claims t
left join (
    select t1."Claim_guid" as Claim_guid,
        max(t1."Receive_date") as Receive_last,
        max(case when t1.document_oid = 2 then t1."Receive_date" end) as recieve_claim
    from document_link t1
    group by t1."Claim_guid"
) t2 on t2.Claim_guid = t."Claim_guid"
order by дата_регистрации desc
limit 20
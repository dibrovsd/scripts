with base as (
    select
        d.id,
        coalesce(d_ins.n_contract, d.n_contract) as "Номер договора",
        coalesce(d_ins.person_lname ||' '|| d_ins.person_fname, d.ins_person_lname ||' '|| d.ins_person_fname) as "ФИО",
        coalesce(d_ins.phone, d.ins_phone) as "Номер телефона",
        (
            select string_agg(cntr.title, ', ')
            from docflow_document4_countries cntr_lnk
            inner join docflow_country cntr on cntr.id = cntr_lnk.country_id
            where cntr_lnk.document4_id = d.id
        ) as "Страны",
        rest_type.title as "Цель поездки",
        d.insured_days as "Застрахованные дни",
        d.s_premium as s_premium_document,
        d_ins.s_premium as s_premium_traveler,
        count(1) over(partition by d.id) as traveler_cnt
    from docflow_p4documentinsurer d_ins
    left join docflow_document4 d on d_ins.document_id = d.id
    inner join base_user user_creator on user_creator.id = d.user_creator_id
    inner join docflow_libtable4_10 rest_type on rest_type.id = d.rest_type_id
    where d.d_issue is not null
        and d.canceled = false
        and d.d_issue between [[env.period.0]] and [[env.period.1]]
)

select
    base.*,
    case
        when s_premium_traveler is not null then base.s_premium_traveler
        when s_premium_document is not null then s_premium_document / traveler_cnt
    end as "Премия ко оплате"
from base

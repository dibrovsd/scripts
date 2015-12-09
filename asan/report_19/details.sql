{{datasets.src.sql}}

select
    bso.id,
    case
        when d.blank_type = 1 then 'ОСАГО'
        when d.blank_type = 2 then 'S7'
        when d.blank_type = 3 then 'Недвижимость'
    end as "Тип БСО",

    blank_serie.title || bso.n_blank as "Номер БСО",
    st.title as "Этап",
    u.last_name || ' ' || u.first_name as "Ответственный",
    inscompany.title as "Страховая компания"
from data d
-- Тянем БСО
join docflow_document1 bso on bso.id = d.document_id
join docflow_libtable1_2 blank_serie on blank_serie.id = bso.blank_serie_id
left join docflow_documentevent1 ev on ev.id = bso.last_event_id
left join docflow_state1 st on st.id = ev.state_to_id
left join base_user u on u.id = ev.user_responsible_id
left join docflow_inscompany inscompany on inscompany.id = bso.inscompany_id
-- Договор по БСО
left join reports.base_sales sales on sales.project_id = bso.contract_project and sales.id = bso.contract_pk
where 1 = 1
and d.d_create = to_date([[get.dt]], 'dd.mm.yyyy')
and d.blank_type = [[get.blank_type]]::integer
and d.user_id = [[get.user_id]]::integer
and d.m = [[get.m]]

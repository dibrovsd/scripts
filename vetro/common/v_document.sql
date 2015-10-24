drop view reports.v_document;


create or replace view reports.v_document as
    select d.id,
           d.d_create,
           d.direction_num,
           city.title as city,
           stoacompany.title ||' '|| stoa.title as stoa,
           stoacompany.title as stoa_company,
           d.direction_get_date,
           d.inspection_date,
           d.inspection_date_real,
           d.repair_date,
           d.repair_date_real,
           d.d_documents_send,
           d.s_repair_glass,
           d.s_repair_work,
           d.incoming_date,
           coalesce(d.s_repair_glass, 0) + coalesce(d.s_repair_work, 0) as s_repair_all,
           case
               when damages.action = 1 and replace_glass.glass_type = 1 then 'Оригинальное'
               when damages.action = 1 and replace_glass.glass_type = 2 then 'Не оригинальное'
           end as replace_glass_glass_type,
           case
               when damages.action = 1 then 'Замена'
               when damages.action = 2 then 'Ремонт'
           end as damages_action,
           case
               when d.city_auto_host_id = 12
                   then 'Москва'
               else 'Регион'
           end as region,
           -- Стекло для замены
           replace_glass.code_euro as gfr_code_euro,
           replace_glass.code_original as gfr_code_original,
           manufacturer.title as gfr_code_manufacturer,
           case
                when in_stock.document_id is not null then 'Есть'
                else 'Нет'
           end as glass_in_stock, -- Одно из стекол есть в наличии
           de.d_create as event_create,
           d.pay_date,
    	   d.pay_sum,
           inscompany.title as inscompany,
           d.auto_createyear,
           auto_mark.title as auto_mark,
           auto_model.title as auto_model,
           d.auto_number,
           d.direction_deductible as deductible,
           handling_type.title as handling_type,
           --
           st.title as state,
           u.last_name ||' '|| u.first_name as responsible,
           curator.last_name ||' '|| curator.first_name as curator,
           --
           d.city_auto_host_id,
           d.city_auto_host_id as city_id,
           d.direction_stoa_id as stoa_id,
           stoa.company_id as stoa_company_id,
           de.user_responsible_id as responsible_id,
           de.state_to_id as state_id,
           event_state2.user_responsible_id as curator_id,
           d.inscompany_id,
           d.handling_type as handling_type_id,
           replace_glass.status as gfr_status_id
    from docflow_document1 d
    left join docflow_documentevent1 de on de.id = d.last_event_id
    left join docflow_state1 st on st.id = de.state_to_id
    left join base_city city on city.id = d.city_auto_host_id
    left join base_stoa stoa on stoa.id = d.direction_stoa_id
    left join base_stoacompany stoacompany on stoacompany.id = stoa.company_id
    left join base_user u on u.id = de.user_responsible_id
    left join docflow_p1fsglassstock replace_glass on replace_glass.document_id = d.id
                                                   and replace_glass.replacement = true
    left join base_glassmanufacturer manufacturer on manufacturer.id = replace_glass.manufacturer_id
    left join base_inscompany inscompany on inscompany.id = d.inscompany_id
    left join base_automark auto_mark on auto_mark.id = d.auto_mark_id
    left join base_automodel auto_model on auto_model.id = d.auto_model_id
    left join reports.handling_type on handling_type.id = d.handling_type
    -- Повреждения
    left join (
        select damages.document_id,
            damages.action,
            row_number() over(partition by damages.document_id order by damages.id) as rn
        from docflow_p1fsdamages damages
    ) damages on damages.document_id = d.id
              and damages.rn = 1
    -- Куратор
    left join (
        select de1.document_id,
            de1.user_responsible_id,
            e1.state_to_id as state_id,
            row_number() over(partition by de1.document_id, e1.state_to_id order by de1.d_create desc) as rn
        from docflow_documentevent1 de1
        join docflow_event1 e1 on e1.id = de1.event_id
        where e1.state_to_id in (2, 24) -- (Приглашение на осмотр, Определение типа стекла по УУ)
    ) event_state2 on event_state2.document_id = d.id
                   and (
                       d.handling_type = 1 and state_id = 2
                       or d.handling_type = 2 and state_id = 24
                   )
                   and event_state2.rn = 1
    -- Стекла под замену, которые есть в наличии
    left join (
        select in_stock.document_id,
            row_number() over(partition by in_stock.document_id order by in_stock.id desc) as rn
        from docflow_p1fsglassstock in_stock
    ) in_stock on in_stock.document_id = d.id and in_stock.rn = 1
    left join base_user curator on curator.id = event_state2.user_responsible_id
    where d.deleted = false

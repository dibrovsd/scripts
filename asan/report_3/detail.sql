select
    t.id,
    t.n_blank,
    lib2.title as blank_serie,
    ins.title as inscompany,
    case t.blank_type
        when 1 then 'Полис ОСАГО'
        when 2 then 'S7'
        when 3 then 'Полис недвижимости'
    end as blank_type,
    t4.title as state,
    t3.last_name as responsible
from docflow_document1 t
join docflow_documentevent1 t1 on t1.id = t.last_event_id
join docflow_event1 t2 on t2.id = t1.event_id
join docflow_state1 t4 on t4.id = t2.state_to_id
join base_user t3 on t3.id = t1.user_responsible_id
--
join docflow_libtable1_2 lib2 on lib2.id = t.blank_serie_id
join docflow_inscompany ins on ins.id = t.inscompany_id
where
    t.deleted = false
    and t.blank_type = {{get.blank_type}}
    and t.blank_status = {{get.status}}
    and t2.state_to_id != 4 -- Сдан в СК
    and t.channel_root_id = [[env.channel]]

    {% if get.inscompany %}
        and t.inscompany_id = {{get.inscompany}}
    {% endif %}

    {% if get.responsible != 'None' %}
        and t1.user_responsible_id = {{get.responsible}}
    {% endif %}

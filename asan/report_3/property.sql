with src as (
    select
        t1.user_responsible_id as responsible_id,
        t3.last_name as responsible,
        t.inscompany_id,
        t.blank_status,
        count(1) as cnt
    from docflow_document1 t
    join docflow_documentevent1 t1 on t1.id = t.last_event_id
    join docflow_event1 t2 on t2.id = t1.event_id
    join base_user t3 on t3.id = t1.user_responsible_id
    join base_channel ch on ch.id = t3.channel_id
    where
        t.deleted = false
        and t.blank_type = 3 -- Недвижимость
        and t2.state_to_id != 4 -- Сдан в СК
        {% if env.channel %}
            and ch.root_id = [[env.channel]]::integer
        {% endif %}
    group by t1.user_responsible_id, t3.last_name, t.inscompany_id, t.blank_status
)


select
    responsible_id,
    responsible,

    {% for ins in datasets.inscompanys.data %}
        sum(case when inscompany_id = {{ins.id}} and blank_status = 1
            then cnt
        end) as не_использован_{{ins.id}},
        --
        sum(case when inscompany_id = {{ins.id}} and blank_status = 2
            then cnt
        end) as использован_{{ins.id}},
        --
        sum(case when inscompany_id = {{ins.id}} and blank_status = 3
            then cnt
        end) as испорчен_{{ins.id}},
    {% endfor %}
    sum(case when blank_status = 1
        then cnt
    end) as не_использован,
    --
    sum(case when blank_status = 2
        then cnt
    end) as использован,
    --
    sum(case when blank_status = 3
        then cnt
    end) as испорчен
from (
    select responsible_id, responsible, inscompany_id, blank_status, cnt from src
    union all
    select
        null as responsible_id, 'Итого' as responsible,
        inscompany_id, blank_status, sum(cnt) as cnt
    from src
    group by inscompany_id, blank_status
) src1
group by responsible_id, responsible
order by responsible_id nulls last

with params as (
    select
        [[env.curator]]::integer as curator,
        [[env.inscompany]]::integer as inscompany,
        [[env.handling_type]]::integer as handling_type,
        [[env.period.0]] as d_start,
        [[env.period.1]] as d_end,

        {% if get.dt == 'incoming' %}
            to_date([[get.d_start]], 'yyyy-mm-dd') as d_start_dt,
            to_date([[get.d_end]], 'yyyy-mm-dd') - interval '1 second' as d_end_dt,

        {% else %}
            null::date as d_start_dt,
            null::date as d_end_dt,

        {% endif %}

        [[get.group_id]]::integer as group_id


        -- 0 as curator,
        -- 0 as inscompany,
        -- current_date - 60 as d_start,
        -- current_date as d_end
),

base_event as (
    select e.state_to_id,
        e.document_id
    from reports.rep_1_last_events e
    cross join params
    where e.d_create between params.d_start and params.d_end
)


select
    d.id,
    d.d_create::date as "Зарегистрирован",
    d.event_create::date as "Отправлен",
    d.direction_num as "Направление",
    d.inscompany as "Страховая компания",
    d.state as "Этап",
    d.city as "Город",
    d.stoa as "СТОА",
    d.handling_type as "Тип обращений",
    d.responsible as "Ответственный",
    d.curator as "Куратор",
    d.direction_get_date::date as "Дата получения направления",
    d.inspection_date::date as "Дата осмотра (план)",
    d.inspection_date_real::date as "Дата осмотра (факт)",
    d.repair_date::date as "Дата ремонта (план)",
    d.repair_date_real::date as "Дата ремонта (факт)",
    d.d_documents_send::date as "Дата передачи оригиналов",
    d.s_repair_all as "Итого по заказ-наряду",

    {% if not 'stoa' in user_params.roles %}
        d.pay_date as "Дата оплаты",
        d.pay_sum as "Сумма оплаты",
    {% endif %}

    d.replace_glass_glass_type as "Вид стекла на замену",
    d.damages_action as "Вид работ"
from reports.v_document d
cross join params
where 1 = 1
    and (params.inscompany = 0 or d.inscompany_id = params.inscompany)
    and (params.curator = 0 or d.curator_id = params.curator)
    and (params.handling_type = 0 or d.handling_type_id = params.handling_type)
    and (params.group_id = 0 or params.group_id = -1 and d.city_id != 12 or d.{{env.group_by}}_id = params.group_id)
    and d.{{env.group_by}}_id is not null

    {% if 'customer_service' in user_params.roles %}
       and d.curator_id = {{user.id}}

    {% elif 'stoa' in user_params.roles %}
       and d.stoa_id in ({{user.stations_ids|join:","}})

    {% endif %}

    {% if get.dt == 'incoming' %}
        and d.d_create between params.d_start and params.d_end
        and d.d_create between params.d_start_dt and params.d_end_dt

    {% elif get.dt == 'out_repair' %}
        and d.repair_date_real between params.d_start and params.d_end

    {% elif get.dt == 'out_wp' %}
        and exists (
            select null from base_event e
            where e.document_id = d.id
            and e.state_to_id = 11
        )

    {% elif get.dt == 'out_pay' %}
        and d.pay_date between params.d_start and params.d_end

    {% elif get.dt == 'out_archive' %}
        and exists (
            select null from base_event e
            where e.document_id = d.id
            and e.state_to_id = 12
        )

    {% endif %}

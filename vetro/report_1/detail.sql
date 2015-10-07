with params as (
    select
        [[env.city_auto_host]]::integer as city_auto_host,
        [[env.direction_stoa]]::integer as direction_stoa,
        [[env.responsible]]::integer as curator,

        to_date([[get.d_from]], 'dd.mm.yyyy') as d_from,
        to_date([[get.d_to]], 'dd.mm.yyyy') + interval '1 day - 1 second' as d_to,
        {% if get.inscompany %}[[get.inscompany]]{% else %}0{% endif %}::integer as inscompany

        -- 0 as city_auto_host,
        -- 0 as direction_stoa,
        -- 0 as curator,
        -- to_date('01.01.2015', 'dd.mm.yyyy') as d_from,
        -- to_date('01.08.2015', 'dd.mm.yyyy') as d_to,
        -- 0 as inscompany
)

select d.id,
       d.d_create::date as "Зарегистрирован",
       d.event_create::date as "Отправлен",
       d.direction_num as "Направление",
       d.inscompany as "Страховая компания",
       d.state as "Этап",
       d.city as "Город",
       d.stoa as "СТОА",
       d.responsible as "Ответственный",
       d.direction_get_date::date as "Дата получения направления",
       d.inspection_date::date as "Дата осмотра (план)",
       d.inspection_date_real::date as "Дата осмотра (факт)",
       d.repair_date::date as "Дата ремонта (план)",
       d.repair_date_real::date as "Дата ремонта (факт)",
       d.d_documents_send::date as "Дата передачи оригиналов",
       d.s_repair_all as "Итого по заказ-наряду",
       d.pay_date as "Дата оплаты",
       d.pay_sum as "Сумма оплаты",
       d.replace_glass_glass_type as "Вид стекла на замену",
       d.damages_action as "Вид работ"
from reports.v_document d
cross join params
where d.inscompany_id is not null
    and (params.city_auto_host = 0 or d.city_auto_host_id = params.city_auto_host)
    and (params.direction_stoa = 0 or d.stoa_id = params.direction_stoa)
    and (params.curator = 0 or d.curator_id = params.curator)
    and (params.inscompany = 0 or d.inscompany_id = params.inscompany)

    {% if 'customer_service' in user_params.roles %}
       and d.curator_id = {{user.id}}

    {% elif 'stoa' in user_params.roles %}
       and d.stoa_id in ({{user.stations_ids|join:","}})
       -- "Ожидание решения СК о смене СТОА" и "Ожидание оплаты"
       and d.state_id not in (15, 11)

    {% endif %}

    {% if get.col == 'in' %}
        and d.d_create between params.d_from and params.d_to

    {% elif get.col == 'out_repair' %}
        and d.repair_date_real between params.d_from and params.d_to

    {% elif get.col == 'out_wait_paymenet' %}
        and exists (
            select null
            from reports.rep_1_last_events e
            where e.document_id = d.id
                and e.state_to_id = 11 -- Ожидание оплаты
                and e.d_create between params.d_from and params.d_to
        )

    {% elif get.col == 'out_archive_payment' %}
        and exists (
            select null
            from reports.rep_1_last_events e
            where e.document_id = d.id
                and e.d_create between params.d_from and params.d_to
                and e.state_to_id = 12 -- Архив
                and e.state_from_id = 11 -- Ожидание оплаты
                and d.pay_date is not null
                and d.pay_sum is not null

        )

    {% elif get.col == 'out_archive_reject' %}
        and exists (
            select null
            from reports.rep_1_last_events e
            where e.document_id = d.id
            and e.d_create between params.d_from and params.d_to
            and e.state_to_id = 12 -- Архив
            and not (
                e.state_from_id = 11 -- Ожидание оплаты
                and d.pay_date is not null
                and d.pay_sum is not null
            )

        )

    {% endif %}

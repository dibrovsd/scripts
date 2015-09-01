with params as (
    select
        [[env.city_auto_host]]::integer as city_auto_host,
        [[env.direction_stoa]]::integer as direction_stoa,
        [[env.responsible]]::integer as responsible,
        [[env.inscompany]]::integer as inscompany,
        [[env.curator]]::integer as curator,

        {% if get.doc_state != 'None' %}[[get.doc_state]]{% else %}null{% endif %}::integer as doc_state,
        {% if get.d_from %}[[get.d_from]]{% else %}null{% endif %}::integer as d_from,
        {% if get.d_to %}[[get.d_to]]{% else %}null{% endif %}::integer as d_to

        -- 0 as city_auto_host,
        -- 0 as direction_stoa,
        -- 0 as responsible,
        -- 0 as inscompany,
        --
        -- 15::integer as doc_state,
        -- null::integer as d_from,
        -- null::integer as d_to
),

documents_base as (
    select
        d.id,
        d.d_create::date as "Зарегистрирован",
        d.event_create::date as "Отправлен",
        --
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
        d.damages_action as "Вид работ",
        --
        current_date - coalesce(d.event_create, d.d_create)::date as "Дней"
    from reports.v_document d
    cross join params
    where d.state_id != 12
      and (params.city_auto_host = 0 or d.city_auto_host_id = params.city_auto_host)
      and (params.direction_stoa = 0 or d.stoa_id = params.direction_stoa)
      and (params.responsible = 0 or d.responsible_id = params.responsible)
      and (params.inscompany = 0 or d.inscompany_id = params.inscompany)
      and (params.curator = 0 or d.curator_id = params.curator)

      and (params.doc_state is null
           or params.doc_state = 0 and d.state_id is null
           or d.state_id = params.doc_state)

        {% if 'customer_service' in user_params.roles %}
           and d.curator_id = {{user.id}}

        {% elif 'stoa' in user_params.roles %}
           and d.stoa_id in ({{user.stations_ids|join:","}})
           -- "Ожидание решения СК о смене СТОА" и "Ожидание оплаты"
           and d.state_id not in (15, 11)

        {% endif %}
)

select
    d.*
from documents_base d
cross join params
where 1 = 1
    and (params.d_from is null or d."Дней" >= params.d_from)
    and (params.d_to is null or d."Дней" <= params.d_to)

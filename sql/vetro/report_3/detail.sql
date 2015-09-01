with params as (
    select
        [[env.city_auto_host]]::integer as city_auto_host,
        [[env.direction_stoa]]::integer as direction_stoa,
        [[env.responsible]]::integer as responsible,
        [[env.curator]]::integer as curator,

        {% if get.period == 'Итого просроченные' %}
            null::date as d_from,
            current_date - interval '1 second' as d_to,

        {% elif get.period|length == 7 %}
            to_date('{{get.period}}', 'yyyy_mm') as d_from,
            to_date('{{get.period}}', 'yyyy_mm') + interval '1 month - 1 second' as d_to,

        {% else %}
            to_date('{{get.period}}', 'dd.mm.yyyy') as d_from,
            to_date('{{get.period}}', 'dd.mm.yyyy') + interval '1 day - 1 second' as d_to,

        {% endif %}

        {% if get.from != '' %}[[get.from]]{% else %}null{% endif %}::integer as days_from,
        {% if get.to != '' %}[[get.to]]{% else %}null{% endif %}::integer as days_to

        -- 0 as city_auto_host,
        -- 0 as direction_stoa,
        -- 0 as responsible,
        -- to_date('01.07.2015', 'dd.mm.yyyy') as d_from,
        -- to_date('01.08.2015', 'dd.mm.yyyy') - interval '1 second' as d_to,
        -- 0::integer as days_from,
        -- 5::integer as days_to
),

documents_base as (
    select
        d.id,
        d.d_create::date as "Зарегистрирован",
        d.event_create::date as "Отправлен",
        --
        d.direction_num as "Направление",
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
        d.replace_glass_glass_type as "Вид стекла на замену",
        d.damages_action as "Вид работ",
        --
        {% if get.detail == 'repair' %}
            d.repair_date::date as d_plan,
            d.repair_date_real::date as d_real
        {% else %}
            d.inspection_date::date as d_plan,
            d.inspection_date_real::date as d_real
        {% endif %}
        -- d.inspection_date as d_plan,
        -- d.inspection_date_real as d_real
    from reports.v_document d
    cross join params
    where (params.city_auto_host = 0 or d.city_auto_host_id = params.city_auto_host)
      and (params.direction_stoa = 0 or d.stoa_id = params.direction_stoa)
      and (params.responsible = 0 or d.responsible_id = params.responsible)
      and (params.curator = 0 or d.curator_id = params.curator)

      {% if 'customer_service' in user_params.roles %}
         and d.curator_id = {{user.id}}

      {% elif 'stoa' in user_params.roles %}
         and d.stoa_id in ({{user.stations_ids|join:","}})

      {% endif %}
),

documents as (
    select
        t.*,
        current_date - t.d_plan::date as "Дней"
    from documents_base t
    cross join params
    where t.d_plan is not null
      and t.d_real is null
      and (params.d_from is null or t.d_plan >= params.d_from)
      and (params.d_to is null or t.d_plan <= params.d_to)
)

select
    d.*
from documents d
cross join params
where (params.days_from is null or d."Дней" >= params.days_from)
  and (params.days_to is null or d."Дней" <= params.days_to)

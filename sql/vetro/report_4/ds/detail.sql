with params as (
    select
        [[env.period.0]] as d_from,
        [[env.period.1]] as d_to,
        [[env.city_auto_host]]::integer as city_auto_host,
        [[env.direction_stoa]]::integer as direction_stoa,
        -- GET переменные
        to_date('{{get.d_start}}', 'dd.mm.yyyy') as get_d_start,
        to_date('{{get.d_end}}', 'dd.mm.yyyy') + interval '1 day - 1 second' as get_d_end,
        '{{get.region}}' as region

        -- 0 as city_auto_host,
        -- 0 as direction_stoa,
        -- 0 as responsible,
        -- to_date('01.05.2015', 'dd.mm.yyyy') as d_from,
        -- to_date('01.08.2015', 'dd.mm.yyyy') - interval '1 second' as d_to,
        -- to_date('01.06.2015', 'dd.mm.yyyy') as get_d_start,
        -- to_date('30.06.2015', 'dd.mm.yyyy') - interval '1 second' as get_d_end
)


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
    d.{{env.period_date}}::date as measure_date
    -- d.repair_date_real::date as measure_date
from reports.v_document d
cross join params
where (params.city_auto_host = 0 or d.city_auto_host_id = params.city_auto_host)
    and (params.direction_stoa = 0 or d.stoa_id = params.direction_stoa)

    and d.{{env.period_date}} between params.d_from and params.d_to
    and d.{{env.period_date}} between params.get_d_start and params.get_d_end
    -- and d.repair_date_real between params.d_from and params.d_to
    -- and d.repair_date_real between params.get_d_start and params.get_d_end

    {% if get.region == 'Москва' %}
        and d.city_auto_host_id = 12

    {% elif get.region == 'Регион' %}
        and d.city_auto_host_id != 12

    {% endif %}

    {% if get.measure == 'replace_not_original_avg' or get.measure == 'replace_not_original_cnt' or get.measure == 'replace_not_original_ratio' %}
        and d.damages_action = 'Замена'
        and d.replace_glass_glass_type = 'Не оригинальное'

    {% elif get.measure == 'replace_original_avg' or get.measure == 'replace_original_cnt' or get.measure == 'replace_original_ratio' %}
        and d.damages_action = 'Замена'
        and d.replace_glass_glass_type = 'Оригинальное'

    {% elif get.measure == 'repair_avg' or get.measure == 'repair_cnt' or get.measure == 'repair_ratio' %}
        and d.damages_action = 'Ремонт'

    {% endif %}

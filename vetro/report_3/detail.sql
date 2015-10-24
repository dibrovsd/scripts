{{datasets.src.sql}}

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
    m.days_expire as "Просроченно"
from measures m
inner join reports.v_document d on d.id = m.id
where m.measure = [[get.detail]]

{% if get.period == 'Итого просроченные' %}
    and m.m_date < current_date

{% elif get.period|length == 7 %}
    and m.m_date between to_date('{{get.period}}', 'yyyy_mm')
                         and to_date('{{get.period}}', 'yyyy_mm') + interval '1 month - 1 second'

{% elif get.period|length == 10 %}
    and m.m_date between to_date('{{get.period}}', 'dd.mm.yyyy')
                         and to_date('{{get.period}}', 'dd.mm.yyyy') + interval '1 day - 1 second'

{% endif %}

{% if get.from != '' %}
    and m.days_expire >= [[get.from]]::integer
{% endif %}

{% if get.to != '' %}
    and m.days_expire <= [[get.to]]::integer
{% endif %}

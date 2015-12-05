{{datasets.src.sql}}


select
    d.id,
    d.d_create::date as "Зарегистрирован",
    d.event_create::date as "Отправлен",
    --
    d.direction_num as "Направление",
    d.inscompany as "Страховая компания",
    d.auto_mark as "Марка ТС",
    d.auto_model as "Модель ТС",
    d.auto_number as "Номер ТС",
    d.handling_type as "Тип обращений",
    d.state as "Этап",
    d.city as "Город",
    d.stoa as "СТОА",
    d.curator as "Куратор",
    d.deductible as "Франшиза",
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
    d1.days as "Дней"
from documents1 d1
inner join reports.v_document d on d.id = d1.id
where 1 = 1

    {% if get.doc_state == '0' %}
        and d1.state_id is null -- Черновик

    {% elif get.doc_state != 'None' %}
        and d1.state_id = [[get.doc_state]]::integer

    {% endif %}

    {% if get.state_measure != '' %}
        and d1.state_measure = [[get.state_measure]]
    {% endif %}

    {% if get.d_from %}
        and d1.days >= [[get.d_from]]::integer
    {% endif %}

    {% if get.d_to %}
        and d1.days <= [[get.d_to]]::integer
    {% endif %}

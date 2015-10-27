{{datasets.src.sql}}

select
    d.id as id,
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
    tsk.d_start::date as "Создана",
    tsk.d_close::date as "Закрыта",
    tsk.days as "Дней"
from tasks1 tsk
join documents_base d1 on d1.id = tsk.document_id
join reports.v_document d on d.id = d1.id
cross join params
where 1 = 1

    {% if get.task_type != 'None' %}
        and tsk.tasktype_id = [[get.task_type]]::integer
    {% endif %}

    {% if get.task_state != 'None' %}
        and tsk.state = [[get.task_state]]::integer
    {% endif %}

    {% if get.glass_in_stock != '' %}
        and d.state_id = 4 and d.glass_in_stock = [[get.glass_in_stock]]
    {% endif %}

    {% if get.d_from %}
        and tsk.days >= [[get.d_from]]::integer
    {% endif %}

    {% if get.d_to %}
        and tsk.days <= [[get.d_to]]::integer
    {% endif %}
order by "Отправлен" desc

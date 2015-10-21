{{datasets.src.sql}}

select
    d.id,
    d.direction_num as "Направление",
    d.state as "Этап",
    d.city as "Город",
    d.stoa as "СТОА",
    d.responsible as "Ответственный",
    d.direction_get_date as "Дата получения направления",
    d.repair_date_real as "Дата ремонта (факт)",
    d.d_documents_send as "Дата передачи оригиналов",
    op.days_documents as "Срок сдачи документов",
    d.pay_date as "Дата оплаты",
    op.days_payment as "Срок оплаты",
    op.days_summary as "Средний срок полного цикла",
    d.s_repair_all as "Итого по заказ-наряду",
    d.replace_glass_glass_type as "Вид стекла на замену",
    d.damages_action as "Вид работ",
    op.days_repair as "Cрок выполнения работ"
from operations op
join reports.v_document d on d.id = op.id
where 1 = 1
    {% if get.action != 'Итого' %}
    and op.action = [[get.action]]
    {% endif %}

    {% if get.region != 'Итого' %}
    and op.region = [[get.region]]
    {% endif %}

    {% if get.glass_type != 'Итого' %}
        and op.glass_type = [[get.glass_type]]
    {% endif %}

    {% if get.m == 'repair' %}
        and op.m = 'send_to_ins'
        {% if get.from %}
            and op.days_repair >= [[get.from]]::integer
        {% endif %}
        {% if get.to  %}
            and op.days_repair <= [[get.to]]::integer
        {% endif %}

    {% else %}
        and op.m = 'pay'
        {% if get.m == 'documents' %}
            and op.days_documents is not null
        {% elif get.m == 'payment' %}
            and op.days_payment is not null
        {% elif get.m == 'summary' %}
            and op.days_summary is not null
        {% endif %}

    {% endif %}

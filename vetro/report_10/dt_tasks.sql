{{datasets.src_task.sql}}

select
    d.id,
    d.d_create::date as "Зарегистрирован",
    d.event_create::date as "Отправлен",
    d.direction_num as "Направление",
    d.inscompany as "Страховая компания",
    d.state as "Этап",
    d.city as "Город",
    d.stoa as "СТОА",
    d.stoa_company as "СТОА (компания)",
    d.responsible as "Ответственный",
    d.curator as "Куратор",
    d.damages_action as "Вид работ",
    d.replace_glass_glass_type as "Вид стекла на замену",
    d.handling_type as "Тип обращений",
    --
    lib.title as "Тип задачи",
    t.d_create as "Создана",
    t.d_close as "Закрыта",
    u.last_name || ' '|| u.first_name as "Оператор"
from df_task_task1 t
inner join reports.v_document d on d.id = t.document_id
inner join df_task_tasktype1 lib on lib.id = t.tasktype_id
inner join base_user u on u.id = t.responsible_id
where exists (
    select null from operations op
    where op.id = t.id

    {% if get.dt == 'task_cnt_in' %}
        and op.m = 'in'
    {% else %}
        and op.m =  'out'
    {% endif %}

    {% if get.user != '0' %}
        and op.user_id = [[get.user]]::integer
    {% endif %}

    {% if get.tasktype != '0' %}
        and op.tasktype_id = [[get.tasktype]]::integer
    {% endif %}
)

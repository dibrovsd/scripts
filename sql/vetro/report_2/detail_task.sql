with params as (
    select
        [[env.city_auto_host]]::integer as city_auto_host,
        [[env.direction_stoa]]::integer as direction_stoa,
        [[env.responsible]]::integer as responsible,
        [[env.inscompany]]::integer as inscompany,
        [[env.curator]]::integer as curator,

        {% if get.task_type != 'None' %}[[get.task_type]]{% else %}null{% endif %}::integer as task_type,
        {% if get.task_state != 'None' %}[[get.task_state]]{% else %}0{% endif %}::integer as task_state,

        {% if get.d_from %}[[get.d_from]]{% else %}null{% endif %}::integer as d_from,
        {% if get.d_to %}[[get.d_to]]{% else %}null{% endif %}::integer as d_to

        -- 0 as city_auto_host,
        -- 0 as direction_stoa,
        -- 0 as responsible,
        -- 0 as inscompany,
        -- null::integer as task_type,
        -- 0::integer as task_state,
        -- 3::integer as d_from,
        -- 5::integer as d_to
),

documents_base as (
    select d.*
    from reports.v_document d
    cross join params
    where d.state_id != 12
      and (params.city_auto_host = 0 or d.city_auto_host_id = params.city_auto_host)
      and (params.direction_stoa = 0 or d.stoa_id = params.direction_stoa)
      and (params.responsible = 0 or d.responsible_id = params.responsible)
      and (params.inscompany = 0 or d.inscompany_id = params.inscompany)
      and (params.curator = 0 or d.curator_id = params.curator)

      {% if not user_params.roles %}
        and (params.curator = 0 or d.curator_id = params.curator)

      {% elif 'customer_service' in user_params.roles %}
         and d.curator_id = {{user.id}}

      {% elif 'stoa' in user_params.roles %}
         and d.stoa_id in ({{user.stations_ids|join:","}})
         -- "Ожидание решения СК о смене СТОА" и "Ожидание оплаты"
         and d.state_id not in (15, 11)
      {% endif %}
),

tasks as (
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
        d.state as "Этап",
        d.city as "Город",
        d.stoa as "СТОА",
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
        tsk.d_create::date as "Создана",
        tsk.d_confirm::date as "Выполнена",
        --
        (coalesce(tsk.d_confirm, current_date)::date - tsk.d_create::date) as "Дней"
    from df_task_task1 tsk
    join documents_base d on d.id = tsk.document_id
    cross join params
    where (params.task_type is null or params.task_type = tsk.tasktype_id)
      and (params.task_state = 0 or params.task_state = tsk.state)


)

select tasks.*
from tasks
cross join params
where 1 = 1
    and (params.d_from is null or tasks."Дней" >= params.d_from)
    and (params.d_to is null or tasks."Дней" <= params.d_to)

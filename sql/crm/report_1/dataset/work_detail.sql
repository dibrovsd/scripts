with params as (
    select
        [[env.period.0]] as d_from,
        [[env.period.1]] as d_to,

        {% if get.status == 'None' %}null{% else %}[[get.status]]{% endif %}::integer as status,
        {% if get.resp == 'None' %}null{% else %}[[get.resp]]{% endif %}::integer as resp


        -- to_date('24.07.2015', 'dd.mm.yyyy') as d_from,
        -- to_date('24.07.2015', 'dd.mm.yyyy') + interval '1 day - 1 second' as d_to,
        -- 35::integer as resp,
        -- null::integer as status
)

select
    tsk.id,
    tsk.d_create as "Дата создания",
    u.last_name ||' '|| u.first_name as "Ответственный",
    lib.title as "Текущий статус",
    tsk_log.d_create as "Дата текущего статуса",
    contr.brand as "Марка",
    contr.model as "Модель",
    contr.insurance_company as "Страховая",
    contr.contract_number as "Номер договора",
    contr.d_end as "Дата окончания договора"
from crm_calltasklog tsk_log
inner join crm_calltask tsk on tsk.id = tsk_log.task_id
inner join base_user u on u.id = tsk.responsible_id
join reports.libs lib
		on lib.lib_name = 'calltask_status'
		and lib.id = tsk.status
inner join crm_inscontractauto contr on contr.id = tsk.contract_id
cross join params
where tsk_log.d_create between params.d_from and params.d_to
    and (params.status is null or tsk.status = params.status)
    and (params.resp is null or tsk.responsible_id = params.resp)

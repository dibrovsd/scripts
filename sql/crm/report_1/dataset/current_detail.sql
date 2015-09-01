with params as (
    select
        [[get.resp]]::integer as responsible_id,
        {% if get.status == 'None' %}null{% else %}[[get.status]]{% endif %}::integer as status

        -- 39 as responsible_id,
        -- 11 as status
)

select
	tsk.id,
	tsk.d_create as "Дата создания",
    u.last_name ||' '|| u.first_name as "Ответственный",
    lib.title as "Текущий статус",
    contr.brand as "Марка",
    contr.model as "Модель",
    contr.insurance_company as "Страховая",
    contr.contract_number as "Номер договора",
    contr.d_end as "Дата окончания договора"
from crm_calltask tsk
inner join base_user u on u.id = tsk.responsible_id
join reports.libs lib
		on lib.lib_name = 'calltask_status'
		and lib.id = tsk.status
inner join crm_inscontractauto contr on contr.id = tsk.contract_id
cross join params
where (params.status is null or tsk.status = params.status)
  and tsk.responsible_id = params.responsible_id

with gr as (
	select
		t.status,
		t.responsible_id,
		count(1) as cnt
	from crm_calltask t
	group by t.status, t.responsible_id
),

gr_cum as (
	select status, responsible_id, cnt from gr
	union all
	select null as status, responsible_id, sum(cnt) as cnt from gr group by responsible_id
)

select
	lib.title as "Этап",
	{% for u in datasets.users.data %}
	sum(case when tsk.responsible_id = {{u.id}} then tsk.cnt end) as "{{u.title}}",
	{% endfor %}
	tsk.status as status_id
from gr_cum tsk
left join reports.libs lib
		on lib.lib_name = 'calltask_status'
		and lib.id = tsk.status
group by tsk.status, lib.title
order by lib.title

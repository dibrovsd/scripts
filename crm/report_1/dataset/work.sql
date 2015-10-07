with params as (
    select
        [[env.period.0]] as d_from,
        [[env.period.1]] + interval '1 day - 1 second' as d_to

        -- to_date('01.01.2015', 'dd.mm.yyyy') as d_from,
        -- to_date('01.08.2015', 'dd.mm.yyyy') as d_to
),

data as (
    select
        u.id as user_id,
        u.last_name ||' '|| u.first_name as user,
        tsk_log.status
    from crm_calltasklog tsk_log
    inner join crm_calltask tsk on tsk.id = tsk_log.task_id
    inner join base_user u on u.id = tsk.responsible_id
    cross join params
    where tsk_log.d_create between params.d_from and params.d_to
),

data_gr as (
    select
        d.user,
        d.status,
        d.user_id,
        count(1) as cnt
    from data d
    group by d.user_id, d.user, d.status
),

data_gr_cum as (
    select
        d.user,
        d.user_id,
        d.status,
        d.cnt
    from data_gr d

    union all

    select
        null as user,
        null as user_id,
        d.status,
        sum(d.cnt) as cnt
    from data_gr d
    group by d.user_id, d.user, d.status
)

select
    d.user,
    {% for s in datasets.statuses.data %}
	sum(case when d.status = {{s.id}} then d.cnt end) as "{{s.title}}",
	{% endfor %}
    sum(d.cnt) as "Итого",
    d.user_id
from data_gr_cum d
group by d.user_id, d.user
order by d.user nulls last

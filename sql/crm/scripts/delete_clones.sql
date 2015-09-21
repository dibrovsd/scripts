delete from crm_calltasklog where task_id in (
	select id
    from crm_calltask
    where client_id in (
    	select
    		max(t.id)
    	from crm_client t
    	group by t.phone
    	having count(1) > 1
    )
);

delete from crm_calltask where client_id in (
	select
		max(t.id)
	from crm_client t
	group by t.phone
	having count(1) > 1
);

delete from crm_inscontractauto where client_id in (
		select
		max(t.id)
	from crm_client t
	group by t.phone
	having count(1) > 1
);

delete from crm_client where id in (
    select
    	max(t.id)
    from crm_client t
    group by t.phone
    having count(1) > 1
);

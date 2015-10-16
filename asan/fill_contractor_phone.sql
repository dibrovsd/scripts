update contractor_contractorperson
set phone = phone_src
from (
	select
		t2.id as id_src,
		t2.ins_phone as phone_src
	from (
		select t.id, t1.ins_phone, row_number() over(partition by t.contractor_id order by t1.d_issue desc) rn
		from contractor_contractorperson t
		inner join reports.base_sales t1 on t1.contractor_id = t.contractor_id
	) t2
	where rn = 1
) t3
where id = id_src

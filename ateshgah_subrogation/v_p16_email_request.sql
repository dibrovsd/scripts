create view reports.v_p16_email_request as
select 
    id,
    case when n_email != '' then n_email else null end as n_email,
    case when d_send_mail != '' then to_date(d_send_mail, 'dd.mm.yyyy') else null end as d_send_mail,
    case when d_result_plan != '' then to_date(d_result_plan, 'dd.mm.yyyy') else null end as d_result_plan,
    case when d_result_fact != '' then to_date(d_result_fact, 'dd.mm.yyyy') else null end as d_result_fact,
    case when refund_type != '' then refund_type::integer else null end as refund_type
from (
    select id,
        (xpath('@n_email', item))[1]::VARCHAR as n_email,
        (xpath('@d_send_mail', item))[1]::VARCHAR as d_send_mail,
        (xpath('@d_result_plan', item))[1]::VARCHAR as d_result_plan,
        (xpath('@d_result_fact', item))[1]::VARCHAR as d_result_fact,
        (xpath('@refund_type', item))[1]::VARCHAR as refund_type
    from (
        select 
            t.id, 
            unnest(xpath('/xml/item', t.email_request::xml)) as item
        from docflow_document16 t
        where t.email_request is not null
    ) t1
) t2
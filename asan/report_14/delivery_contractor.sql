with params as (
    select
        [[get.delivery_contractor]]::integer as contractor_id,
        {% if env.d_issue %}[[env.d_issue.0]]{% else %}null::date{% endif %} as d_issue_from,
        {% if env.d_issue %}[[env.d_issue.1]]{% else %}null::date{% endif %} as d_issue_to,
        {% if env.delivery_date %}[[env.delivery_date.0]]{% else %}null::date{% endif %} as delivery_date_from,
        {% if env.delivery_date %}[[env.delivery_date.1]]{% else %}null::date{% endif %} as delivery_date_to

        -- 4449::integer as contractor_id
)

select
    city.title as city,
    s.ins_person,
    s.delivery_region,
    s.delivery_address,
    s.delivery_date,
    s.delivery_time_from ||' '|| s.delivery_time_to as delivery_time,
    s.delivery_comments,
    s.ins_phone
from reports.base_sales s
cross join params
left join docflow_city city on city.id = s.delivery_city_id
where s.seller_territory_id = 9
  and s.contractor_id = params.contractor_id
  --
  and (params.d_issue_from is null
      or s.d_issue between params.d_issue_from and params.d_issue_to)
  and (params.delivery_date_from is null
      or s.delivery_date between params.delivery_date_from and params.delivery_date_to)

order by s.delivery_date desc
limit 1

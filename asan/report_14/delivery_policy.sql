with params as (
    select
        {% if env.d_issue %}[[env.d_issue.0]]{% else %}null::date{% endif %} as d_issue_from,
        {% if env.d_issue %}[[env.d_issue.1]]{% else %}null::date{% endif %} as d_issue_to,
        {% if env.delivery_date %}[[env.delivery_date.0]]{% else %}null::date{% endif %} as delivery_date_from,
        {% if env.delivery_date %}[[env.delivery_date.1]]{% else %}null::date{% endif %} as delivery_date_to,
        [[get.delivery_contractor]]::integer as contractor_id

        -- to_date('01.05.2014', 'dd.mm.yyyy') as d_issue_from,
        -- to_date('31.05.2016', 'dd.mm.yyyy') as d_issue_to,
        -- to_date('01.05.2014', 'dd.mm.yyyy') as delivery_date_from,
        -- to_date('31.05.2016', 'dd.mm.yyyy') as delivery_date_to
),

sales as (
    select
        s.n_contract as "Договор",
        s.product as "Продукт",
        s.s_premium - s.s_discount as "Итоговая премия"
    from reports.base_sales s
    cross join params
    where s.channel_root_id = 9
      and s.contractor_id = params.contractor_id
      and (params.d_issue_from is null or s.d_issue between params.d_issue_from and params.d_issue_to)
      and (params.delivery_date_from is null or s.delivery_date between params.delivery_date_from
                                                                        and params.delivery_date_to)
)

select
    "Договор",
    "Продукт",
    "Итоговая премия"
from sales

union all

select
    null,
    null,
    sum("Итоговая премия")
from sales

order by 1, 2

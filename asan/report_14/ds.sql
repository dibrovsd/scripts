with params as (
    select
        {% if env.d_issue %}[[env.d_issue.0]]{% else %}null::date{% endif %} as d_issue_from,
        {% if env.d_issue %}[[env.d_issue.1]]{% else %}null::date{% endif %} as d_issue_to,
        {% if env.delivery_date %}[[env.delivery_date.0]]{% else %}null::date{% endif %} as delivery_date_from,
        {% if env.delivery_date %}[[env.delivery_date.1]]{% else %}null::date{% endif %} as delivery_date_to

        -- to_date('01.05.2014', 'dd.mm.yyyy') as d_issue_from,
        -- to_date('31.05.2016', 'dd.mm.yyyy') as d_issue_to,
        -- to_date('01.05.2014', 'dd.mm.yyyy') as delivery_date_from,
        -- to_date('31.05.2016', 'dd.mm.yyyy') as delivery_date_to
)

select
    s.id,
    s.project_id,
    s.ins_person,
    s.product,
    s.ins_phone,
    s.n_contract,
    inscompany.title as inscompany,
    bso.n_blank as bso,
    s7.n_blank as s7,
    s.delivery_date,
    s.delivery_time_from ||' '|| s.delivery_time_to as delivery_time,
    city.title,
    s.delivery_region,
    s.delivery_address,
    s.delivery_comments,
    s.contractor_id,
    --
    'Print' as print_title,
    case
        when s.product in ('Пятерочка', 'ОСАГО+', 'Супер КАСКО')
            then null
        when s.product = 'Уверенный водитель'
            then '/docflow/plugins/df_docgen/get_document/11/'|| s.id ||'/?template=1'
        when s.product = 'Просто КАСКО'
            then '/docflow/p12/get_contract_pdf/'|| s.id ||'/'
        when s.project_id = 4
            then '/docflow/p4/get_contract_pdf/'||s.id||'/'
        else '/docflow/plugins/df_docgen/get_document/'|| s.project_id|| '/'|| s.id ||'/?template=1'
    end as print_url,
    -- --
    'Delivery' as delivery_title,
    'Распечатано' || case when info.status in ('prined', 'courier') then ' - ОК' else '' end as printed_title,
    'У курьера' || case when info.status = 'courier' then ' - ОК' else '' end as courier_title

--
from reports.base_sales s
inner join base_user u on u.id = s.seller_id
left join docflow_city city on city.id = s.delivery_city_id
left join docflow_document1 bso on bso.id = s.bso_id
left join docflow_document1 s7 on s7.id = s.s7_id
left join docflow_inscompany inscompany on inscompany.id = s.inscompany_id
left join base_policydelivery info on info.project_id = s.project_id
                                   and info.document_id = s.id
cross join params
where s.seller_territory_id = 9
  and (params.d_issue_from is null or s.d_issue between params.d_issue_from and params.d_issue_to)
  and (params.delivery_date_from is null or s.delivery_date between params.delivery_date_from
                                                                    and params.delivery_date_to)

  {% if env.hide_printed %}
    and (info.status is null or info.status not in ('prined', 'courier'))
  {% endif %}

  {% if env.hide_courier %}
    and (info.status is null or info.status != 'courier')
  {% endif %}

order by s.ins_person, s.delivery_date
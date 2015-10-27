with params as (
    select
        [[env.period.0]] as d_start,
        [[env.period.1]] as d_end,
        [[env.city_auto_host]]::integer as city_auto_host,
        [[env.direction_stoa]]::integer as direction_stoa,
        [[env.handling_type]]::integer as handling_type,
        [[env.stoa_company]]::integer as stoa_company,
        [[env.curator]]::integer as curator,
        [[env.inscompany]]::integer as inscompany

        -- to_date('01.07.2015', 'dd.mm.yyyy') as d_start,
        -- to_date('01.08.2015', 'dd.mm.yyyy') - interval '1 second' as d_end,
        -- 0 as city_auto_host,
        -- 0 as direction_stoa,
        -- 0 as stoa_company,
        -- 0 as curator,
        -- 0 as inscompany
),

-- База
documents as (
    select
        d.id,
        --
        d.damages_action as action,
        d.region,
        d.replace_glass_glass_type as glass_type,
        --
        d.d_documents_send,
        d.repair_date_real,
        d.pay_date,
        --
        round(d.repair_date_real::date - d.direction_get_date::date) as days_repair, -- До ремонта
        round(d.d_documents_send::date - d.repair_date_real::date) as days_documents, -- До передачи документов в СК
        round(d.pay_date::date - d.d_documents_send::date) as days_payment, -- До оплаты
        round(d.pay_date::date - d.direction_get_date::date) as days_summary -- Полный цикл
    from reports.v_document d
    cross join params
    where (params.city_auto_host = 0 or d.city_auto_host_id = params.city_auto_host)
      and (params.direction_stoa = 0 or d.stoa_id = params.direction_stoa)
      and (params.stoa_company = 0 or d.stoa_company_id = params.stoa_company)
      and (params.curator = 0 or d.curator_id = params.curator)
      and (params.inscompany = 0 or d.inscompany_id = params.inscompany)
      and (params.handling_type = 0 or d.handling_type_id = params.handling_type)
),

operations as (
    select
        'send_to_ins' as m,
        --
        d.action,
        d.region,
        d.glass_type,
        --
        d.id,
        d.days_repair,
        d.days_documents,
        d.days_payment,
        d.days_summary
    from documents d
    cross join params
    where d.repair_date_real between params.d_start and params.d_end

    union all

    select
        'pay' as m,
        --
        d.action,
        d.region,
        d.glass_type,
        --
        d.id,
        d.days_repair,
        d.days_documents,
        d.days_payment,
        d.days_summary
    from documents d
    cross join params
    where d.pay_date between params.d_start and params.d_end
)

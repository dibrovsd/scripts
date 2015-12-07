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
        d.handling_type_id,
        --
        case
            when d.handling_type_id = 1 then d.direction_get_date -- Направление
            when d.handling_type_id = 2 then d.direction_date -- УУ
        end as d_start,
        d.direction_date,
        d.d_documents_send,
        d.repair_date_real,
        d.pay_date,
        --
        round(d.repair_date_real::date - d.direction_date::date) as days_repair, -- До ремонта
        round(d.d_documents_send::date - d.repair_date_real::date) as days_documents, -- До передачи документов в СК
        round(d.pay_date::date - d.d_documents_send::date) as days_payment, -- До оплаты
        round(d.pay_date::date - d.direction_date::date) as days_summary -- Полный цикл
    from reports.v_document d
    cross join params
    where (params.city_auto_host = 0 or d.city_auto_host_id = params.city_auto_host)
      and (params.direction_stoa = 0 or d.stoa_id = params.direction_stoa)
      and (params.stoa_company = 0 or d.stoa_company_id = params.stoa_company)
      and (params.curator = 0 or d.curator_id = params.curator)
      and (params.inscompany = 0 or d.inscompany_id = params.inscompany)
      and (params.handling_type = 0 or d.handling_type_id = params.handling_type)
),

_operations as (
    -- Средний срок выполнения работ
    select
        'repair' as m,
        --
        d.d_start::date as d_start,
        d.repair_date_real::date as d_end,
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

    -- Средний срок сдачи документов
    select
        'to_inscompany' as m,
        --
        d.repair_date_real::date as d_start,
        d.d_documents_send::date as d_end,
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
    where d.d_documents_send between params.d_start and params.d_end

    union all

    -- Средний срок оплаты
    select
        'pay' as m,
        --
        d.d_documents_send::date as d_start,
        d.pay_date::date as d_end,
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

    union all

    -- Средний срок полного цикла
    select
        'full_process' as m,
        --
        d.d_start::date as d_start,
        d.pay_date::date as d_end,
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
),

operations as (
    select op.*,
        op.d_end - op.d_start as days
    from _operations op
    where op.d_start <= op.d_end
),

_struct as (
    select 'ЗАМЕНА' as title,
        'background-color: #DCDCDC; font-weight: bold;' as row_style,
        null as glass_type,
        null as action,
        null as region

    union all

    select 'Москва' as title,
        'background-color: #FFDAB9; font-weight: bold;' as row_style,
        null as glass_type,
        null as action,
        null as region

    union all

    select 'Не оригинальное стекло' as title,
        '' as row_style,
        'Не оригинальное' as glass_type,
        'Замена' as action,
        'Москва' as region

    union all

    select 'Оригинальное стекло' as title,
        '' as row_style,
        'Оригинальное' as glass_type,
        'Замена' as action,
        'Москва' as region

    union all

    select 'Все стекла' as title,
        '' as row_style,
        'Итого' as glass_type,
        'Замена' as action,
        'Москва' as region

        union all

    select 'Регионы' as title,
        'background-color: #FFDAB9; font-weight: bold;' as row_style,
        null as glass_type,
        null as action,
        null as region

    union all

    select 'Не оригинальное стекло' as title,
        '' as row_style,
        'Не оригинальное' as glass_type,
        'Замена' as action,
        'Регион' as region

    union all

    select 'Оригинальное стекло' as title,
        '' as row_style,
        'Оригинальное' as glass_type,
        'Замена' as action,
        'Регион' as region

    union all

    select 'Все стекла' as title,
        '' as row_style,
        'Итого' as glass_type,
        'Замена' as action,
        'Регион' as region

    union all

    select 'Москва/Регионы' as title,
        'background-color: #FFDAB9; font-weight: bold;' as row_style,
        null as glass_type,
        null as action,
        null as region

    union all

    select 'Не оригинальное стекло' as title,
        '' as row_style,
        'Не оригинальное' as glass_type,
        'Замена' as action,
        'Итого' as region

    union all

    select 'Оригинальное стекло' as title,
        '' as row_style,
        'Оригинальное' as glass_type,
        'Замена' as action,
        'Итого' as region

    union all

    select 'Все стекла' as title,
        '' as row_style,
        'Итого' as glass_type,
        'Замена' as action,
        'Итого' as region

    union all

    select 'РЕМОНТ' as title,
        'background-color: #DCDCDC; font-weight: bold;' as row_style,
        null as glass_type,
        null as action,
        null as region

    union all

    select 'Москва' as title,
        '' as row_style,
        'Итого' as glass_type,
        'Ремонт' as action,
        'Москва' as region

    union all

    select 'Регион' as title,
        '' as row_style,
        'Итого' as glass_type,
        'Ремонт' as action,
        'Регион' as region

    union all

    select 'Москва/Регионы' as title,
        '' as row_style,
        'Итого' as glass_type,
        'Ремонт' as action,
        'Итого' as region

    union all

    select 'ИТОГО ЗАМЕНА/РЕМОНТ' as title,
        'background-color: #DCDCDC; font-weight: bold;' as row_style,
        null as glass_type,
        null as action,
        null as region

    union all

    select 'Москва' as title,
        '' as row_style,
        'Итого' as glass_type,
        'Итого' as action,
        'Москва' as region

    union all

    select 'Регион' as title,
        '' as row_style,
        'Итого' as glass_type,
        'Итого' as action,
        'Регион' as region

    union all

    select 'Москва/Регионы' as title,
        '' as row_style,
        'Итого' as glass_type,
        'Итого' as action,
        'Итого' as region
),

struct as (
    select s.*, row_number() over() as n_order from _struct s
)

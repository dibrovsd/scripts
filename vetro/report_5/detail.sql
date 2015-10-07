with params as (
    select
        [[env.period.0]] as d_start,
        [[env.period.1]] as d_end,
        [[env.city_auto_host]]::integer as city_auto_host,
        [[env.direction_stoa]]::integer as direction_stoa,
        [[env.stoa_company]]::integer as stoa_company,
        [[env.curator]]::integer as curator,
        [[env.inscompany]]::integer as inscompany

        -- 0 as city_auto_host,
        -- 0 as direction_stoa,
        -- 0 as responsible,
        -- to_date('01.05.2015', 'dd.mm.yyyy') as d_from,
        -- to_date('01.08.2015', 'dd.mm.yyyy') - interval '1 second' as d_to,
        -- to_date('01.06.2015', 'dd.mm.yyyy') as get_d_start,
        -- to_date('30.06.2015', 'dd.mm.yyyy') - interval '1 second' as get_d_end
),

step1 as (
    select
        d.id,
        d.direction_num as "Направление",
        d.state as "Этап",
        d.city as "Город",
        d.stoa as "СТОА",
        d.responsible as "Ответственный",
        d.direction_get_date::date as "Дата получения направления",
        d.repair_date_real::date as "Дата ремонта (факт)",
        d.d_documents_send::date as "Дата передачи оригиналов",
        round(d.d_documents_send::date - d.repair_date_real::date) as "Срок сдачи документов",
        d.pay_date::date as "Дата оплаты",
        round(d.pay_date::date - d.d_documents_send::date) as "Срок оплаты",
        round(d.pay_date::date - d.direction_get_date::date) as "Средний срок полного цикла",
        d.s_repair_all as "Итого по заказ-наряду",
        d.replace_glass_glass_type as "Вид стекла на замену",
        d.damages_action as "Вид работ",
        round(d.repair_date_real::date - d.direction_get_date::date) as "До ремонта"
    from reports.v_document d
    cross join params
    where (params.city_auto_host = 0 or d.city_auto_host_id = params.city_auto_host)
        and (params.direction_stoa = 0 or d.stoa_id = params.direction_stoa)
        and (params.curator = 0 or d.curator_id = params.curator)
        and (params.inscompany = 0 or d.inscompany_id = params.inscompany)
        and (params.stoa_company = 0 or d.stoa_company_id = params.stoa_company)

        and d.repair_date_real between params.d_start and params.d_end

        -- Регион
        {% if get.region == 'Москва' %}
            and d.city_auto_host_id = 12

        {% elif get.region == 'Регион' %}
            and d.city_auto_host_id != 12

        {% endif %}

        -- Вид работ
        {% if get.action and get.action != 'Итого' %}
            and d.damages_action = [[get.action]]
        {% endif %}

        -- Тип стекла
        {% if get.glass_type and get.glass_type != '----' and get.glass_type != 'Итого' %}
            and d.replace_glass_glass_type = [[get.glass_type]]
        {% endif %}
),

step2 as (
    select d.*
    from step1 d
    where 1 = 1

    {% if get.m == 'repair' %}
        {% if get.from %}
            and d."До ремонта" >= [[get.from]]
        {% endif %}
        {% if get.to %}
            and d."До ремонта" <= [[get.to]]
        {% endif %}

    {% elif get.m == 'documents' %}
        and d."Срок сдачи документов" is not null

    {% elif get.m == 'payment' %}
        and d."Срок оплаты" is not null

    {% elif get.m == 'summary' %}
        and d."Средний срок полного цикла" is not null

    {% endif %}
)

select * from step2

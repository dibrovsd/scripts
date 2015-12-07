with params as (
    select
        [[env.city]]::integer as city,
        [[env.stoa_company]]::integer as stoa_company,
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
        d.deductible as "Франшиза",
        d.direction_get_date::date as "Дата получения направления",
        d.repair_date_real::date as "Дата ремонта (факт)",
        d.d_documents_send::date as "Дата передачи оригиналов",
        round(d.d_documents_send::date - d.repair_date_real::date) as "Срок сдачи документов",
        d.pay_date::date as "Дата оплаты",
        round(d.pay_date::date - d.d_documents_send::date) as "Срок оплаты",
        d.s_repair_all as "Итого по заказ-наряду",
        d.replace_glass_glass_type as "Вид стекла на замену",
        d.damages_action as "Вид работ",
        round(d.repair_date_real::date - d.direction_get_date::date) as "До ремонта",
        (current_date - d.d_documents_send::date) as "Календарных дней",
        f_workdays(d.d_documents_send::date, current_date) as "Рабочих дней"
    from reports.v_document d
    cross join params
    where d.d_documents_send is not null
        and d.pay_date is null
        and (params.city = 0 or d.city_auto_host_id = params.city)
        and (params.stoa_company = 0 or d.stoa_company_id = params.stoa_company)
        and (params.inscompany = 0 or d.inscompany_id = params.inscompany)

        -- Регион
        {% if get.region == 'Москва' %}
            and d.city_auto_host_id = 12

        {% elif get.region == 'Регион' %}
            and d.city_auto_host_id != 12

        {% endif %}

        -- Вид работ
        {% if get.inscompany != '-1' %}
            and d.inscompany_id = [[get.inscompany]]::integer
        {% endif %}

)
select
    d.*
from step1 d
where 1 = 1
    {% if get.m == 'lte_15' %}
        and d."Рабочих дней" <= 15

    {% elif get.m == 'gt_15' %}
        and d."Рабочих дней" > 15

    {% elif get.m == 'gt_30' %}
        and d."Календарных дней" > 30

    {% elif get.m == 'gt_60' %}
        and d."Календарных дней" > 60

    {% endif %}

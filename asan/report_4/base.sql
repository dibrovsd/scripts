-- Базовый расчет
with t0 as (
    select
        t.filial,
        t.d_start,
        t.insurer_fio,
        t.tem_months,
        t.currency,
        t.s_kredit,
        case
            when t.tem_months <= 24 then 0.01
            when t.tem_months <= 36 then 0.015
            else 0.02
        end as tariff,
        case
            when t.tem_months <= 3 then 0.0007
            when t.tem_months <= 6 then 0.0015
            when t.tem_months <= 12 then 0.0023
            when t.tem_months <= 18 then 0.0033
            when t.tem_months <= 24 then 0.0044
            when t.tem_months <= 30 then 0.0055
            when t.tem_months <= 36 then 0.0066
            when t.tem_months <= 48 then 0.0088
            when t.tem_months <= 60 then 0.0110
        end as ratio_netto,
        case
            when t.currency != 'AZN'
                then t.s_kredit * t1.curs
            else
                t.s_kredit
        end as s_insurance,
        t.id_client
    from reports.rep4_upload t
    left join base_currencycurs t1
        on t1.date = t.d_start
        and t1.code = t.currency
    where t.filial is not null
),

-- Минимальная сумма кредита в AZN
t01 as (
    select t0.*,
        case
            when t0.currency = 'AZN' then 12
            else 15 * t1.curs
        end as min_premium
    from t0
    join base_currencycurs t1
        on t1.date = t0.d_start
        and t1.code = 'USD'
),

-- Считаем премию
t1 as (
    select t01.*,
        round(greatest((s_insurance * tariff), min_premium)::numeric, 2) as s_premium
    from t01
),

-- Считаем АТА
t2 as (
    select t1.*,
        s_premium * 0.7 as s_ata
    from t1
),

-- Считаем все остальное
t3 as (
    select t2.*,
        (s_ata - (s_insurance * ratio_netto)) * 0.9 as s_cash,
        s_premium * 0.3 as s_comission
    from t2
)

select * from t3

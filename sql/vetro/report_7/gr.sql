/**
* Считает работу куратора по отчетному периоду
*/
with params as (
    select
        -- [[env.city]]::integer as city,
        -- [[env.stoa_company]]::integer as stoa_company,
        -- [[env.curator]]::integer as curator,
        -- [[env.inscompany]]::integer as inscompany,
        --
        -- {# period_date #}
        -- [[env.period.0]] as d_start,
        -- [[env.period.1]] as d_end

        0 as city,
        0 as stoa_company,
        0 as curator,
        0 as inscompany,
        to_date('01.07.2015', 'dd.mm.yyyy') as d_start,
        to_date('01.08.2015', 'dd.mm.yyyy') - interval '1 second' as d_end
),

base as (
    select d.curator_id,
        case when d.d_create between params.d_start and params.d_end then true end as registered_period,
        case when d.repair_date_real between params.d_start and params.d_end then true end as repaired_period,
        d.repair_date_real::date - d.direction_get_date::date as repair_days,
        -- "Передача оригиналов в СК", "Ожидание оплаты" и "Архив", "Согласование счета с СК"
        case when d.state_id in (13, 11, 12, 17) then true end as final_state,
        d.s_repair_all,
        case when d.replace_glass_glass_type = 'Оригинальное' then true else false end as original
    from reports.v_document d
    cross join params
),

gr as (
    select d.curator_id,
        null as at_start,
        null as at_end,
        count(case when registered_period then 1 end) as registered,
        count(case when repaired_period then 1 end) as repaired,
        avg(repair_days) as repair_days,
        count(case when repaired_period and final_state and original then 1 end) as repaired_final_originals,
        count(case when repaired_period and final_state then 1 end) as repaired_final,
        sum(s_repair_all) as s_repair_all
    from base d
    group by d.curator_id
)

select
    gr.at_start,
    gr.at_end,
    gr.registered,
    gr.repaired,
    gr.repair_days,
    f_division(gr.s_repair_all, gr.repaired_final) as avg_loss,
    gr.repaired_final_originals,
    f_division(gr.repaired_final_originals, gr.repaired_final) as originals_ratio
from gr

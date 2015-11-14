-- Параметры
with params as (
    select
        {# Даты  #}
        {% if not env.period_type or env.period_type == 'month' %}
            date_trunc('month', current_date) as d_start,
            current_date + interval '1 day - 1 second' as d_end,

        {% else %}
           [[env.period.0]] as d_start,
           [[env.period.1]] as d_end,

        {% endif %}

        [[env.group_by]]::varchar as trunc_by,
        interval '1 {{env.group_by}}' as interv,
        [[env.inscompany]]::int as inscompany,

        {# Территория #}
        {% if env.seller_territory == 'call_centre' %}
            'call_center'::varchar as territory,
            1::numeric as exp_divider,

        {% elif env.seller_territory == 'asan' %}
            'asan'::varchar as territory,

            {% if env.territory_id %}
                10::numeric as exp_divider,
            {% else %}
                1::numeric as exp_divider,
            {% endif %}

        {% else %}
            null::varchar as territory,
            1::numeric as exp_divider,

        {% endif %}

        {% if env.territory_id %}
            [[env.territory_id]]::int as territory_id
        {% else %}
            null::int as territory_id
        {% endif %}


        -- to_date('01.08.2015', 'dd.mm.yyyy')::date as d_start,
        -- (to_date('31.08.2015', 'dd.mm.yyyy') + interval '1 day - 1 second')::date as d_end,
        -- 'week'::varchar as trunc_by,
        -- interval '1 week' as interv,
        -- 0 as inscompany,
        -- 'asan'::varchar as territory,
        -- null::integer as territory_id,
        -- 1::numeric as exp_divider
),

expenses as (
    select
        dt,
        sum(case when t.expense_type = 'capital' then t.exp_value end) as capital,
        sum(case when t.expense_type = 'recurrent' then t.exp_value end) as recurrent,
        sum(case when t.expense_type = 'other' then t.exp_value end) as other,
        sum(t.exp_value) as summary
    from (
        select
            date_trunc(params.trunc_by, exp.day)::date as dt,
            exp.expense_type,
            sum(exp.value / params.exp_divider) as exp_value
        from reports.base_expense exp
        cross join params
        where exp.day between params.d_start and params.d_end
            and (params.territory is null or exp.segment = params.territory)

            {% if 'call_center' in user_params.territory_only %}
                and exp.segment = 'call_center'
            {% elif 'asan' in user_params.territory_only %}
                and exp.segment = 'asan'
            {% endif %}

        group by date_trunc(params.trunc_by, exp.day), exp.expense_type
    ) t
    group by dt
),

expenses_gr as (
    select
        exp.dt,
        exp.capital,
        exp.recurrent,
        exp.other,
        exp.summary
    from expenses exp

    union all

    select
        null as dt,
        sum(exp.capital) as capital,
        sum(exp.recurrent) as recurrent,
        sum(exp.other) as other,
        sum(exp.summary) as summary
    from expenses exp
),

sales as (
    select
        date_trunc(params.trunc_by, s.d_issue) as dt,
        s.s_comission,
        s.s_premium,
        s.seller_territory_id,
        s.s_discount,
        s.product
    from reports.base_sales s
    cross join params
    where s.d_issue between params.d_start and params.d_end
        and (params.inscompany = 0 or s.inscompany_id = params.inscompany)
        and (
            params.territory is null
            or params.territory = 'asan' and s.seller_territory_id != 9
                and (
                    params.territory_id is null
                    or s.seller_territory_id = params.territory_id
                )
            or params.territory = 'call_center' and s.seller_territory_id = 9
        )

        {% if 'call_center' in user_params.territory_only %}
            and s.seller_territory_id = 9
        {% elif 'asan' in user_params.territory_only %}
            and s.seller_territory_id != 9
        {% endif %}
),

sales_gr as (
    select
        date_trunc(params.trunc_by, t.dt)::date as dt,
        count(1) as cnt,
        -- ОСАГО
        count(case when product = 'ОСАГО' then 1 end) as cnt_osago,
        count(case when product = 'ОСАГО' and s_discount > 0 then 1 end) as cnt_osago_discount,
        sum(case when product = 'ОСАГО' and s_discount > 0 then s_discount end) as s_osago_discount,
        --
        sum(t.s_comission) as s_comission
    from sales t
    cross join params
    group by date_trunc(params.trunc_by, t.dt)::date, params.trunc_by
),

-- Добавим итоги
sales_gr2 as (
    select
        dt,
        cnt,
        cnt_osago,
        cnt_osago_discount,
        s_osago_discount,
        s_comission
    from sales_gr

    union all

    select
        null as dt,
        sum(cnt) as cnt,
        sum(cnt_osago) as cnt_osago,
        sum(cnt_osago_discount) as cnt_osago_discount,
        sum(s_osago_discount) as s_osago_discount,
        sum(s_comission) as s_comission
    from sales_gr
),

sales_gr3 as (
    select s.*,
        --
        exp.capital as exp_capital,
        exp.recurrent as exp_recurrent,
        exp.other as exp_other,
        exp.summary as exp_summary,
        --
        s.s_comission - exp.summary as s_profit,
        case
            when s.s_comission >= exp.summary then 'background-color: #dff0d8;'
            when s.s_comission < exp.summary then 'background-color: #f2dede;'
        end as s_profit_style
    from sales_gr2 s
    left join expenses_gr exp on exp.dt = s.dt
                              or exp.dt is null and s.dt is null
)

select
    s.*,
    case when s_profit > 0 then s_profit * 0.2 else 0 end as profit_tax,
    case when s_profit > 0 then s_profit * 0.8 else 0 end as profit_net
from sales_gr3 s
order by dt

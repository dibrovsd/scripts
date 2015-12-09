-- Параметры
with params as (
    select
       {% if not env.period_type or env.period_type == 'month' %}
            date_trunc('month', current_date) as d_start,
            current_date + interval '1 day - 1 second' as d_end

        {% else %}
           [[env.period.0]] as d_start,
           [[env.period.1]] as d_end

        {% endif %}

        -- to_date('01.01.2015', 'dd.mm.yyyy') as d_start,
        -- current_date as d_end

),

gr as (

    -- ОСАГО
    select
        t.inscompany_id,
        sum(t.s_premium) as s_premium
    from reports.base_realty t
    cross join params
    where t.d_issue between params.d_start and params.d_end
        {% if env.channel %}
            and t.channel_root_id = [[env.channel]]::integer
        {% endif %}

        {% if 'call_center' in user_params.territory_only %}
            and t.channel_root_id = 9
        {% elif 'asan' in user_params.territory_only %}
            and t.channel_root_id = 7
        {% endif %}
    group by inscompany_id

)

select
    i.title as inscompany,
    c.color,
    s_premium
from gr
join docflow_inscompany i on i.id = gr.inscompany_id
left join (
    {{datasets.inscompany_color.sql}}
) c on c.id = gr.inscompany_id
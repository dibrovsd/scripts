with params as (
    select
        [[env.period.0]] as d_from,
        [[env.period.1]] as d_to,
        [[env.seller]]::integer as seller

        -- current_date - 90 as d_from,
        -- current_date as d_to,
        -- 0 as seller
),

-- Группированные данные по id
gr as (
    select
        t.channel_territory_id as channel_id,
        t.seller_id,
        --
        count(case when t.project_id = 3 then 1 end) as cnt_property,
        count(case when t.project_id = 3 and q.auto_number != '' then 1 end) as cnt_property_with_auto,
        --
        count(case when t.project_id = 4 then 1 end) as cnt_travel,
        count(case when t.project_id = 4 and q.auto_number != '' then 1 end) as cnt_travel_with_auto
    from reports.base_sales t
    left join contractor_questionnaire q on q.project_id = t.project_id
                                         and q.document_id = t.id
    cross join params
    where t.channel_root_id = 7 -- АСАН
        and t.project_id in (3, 4)
        and t.d_issue between params.d_from and params.d_to
        and (params.seller = 0 or params.seller = t.seller_id)

        {% if env.channel %}
            and t.channel_territory_id in ({{env.channel|join:","}})
        {% endif %}
    group by t.channel_territory_id, t.seller_id
)



select
    ch.title as channel,
    u.last_name ||' '|| u.first_name as seller,
    --
    gr.cnt_property,
    round(f_division(gr.cnt_property_with_auto, gr.cnt_property) * 100) || '%' as property_ratio,
    --
    gr.cnt_travel,
    round(f_division(gr.cnt_travel_with_auto, gr.cnt_travel) * 100) || '%' as travel_ratio,
    --
    gr.seller_id,
    gr.channel_id
from gr
left join base_channel ch on ch.id = gr.channel_id
left join base_user u on u.id = gr.seller_id

union all

select
    null as channel,
    null as seller,
    --
    sum(gr.cnt_property),
    round(f_division(sum(gr.cnt_property_with_auto), sum(gr.cnt_property)) * 100) || '%' as property_ratio,
    --
    sum(gr.cnt_travel),
    round(f_division(sum(gr.cnt_travel_with_auto), sum(gr.cnt_travel)) * 100) || '%' as travel_ratio,
    --
    0 as seller_id,
    0 as channel_id
from gr

order by channel nulls last, seller nulls last

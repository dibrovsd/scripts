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
        t.seller_territory_id as territory_id,
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
    where t.seller_territory_id != 9
        and t.project_id in (3, 4)
        and t.d_issue between params.d_from and params.d_to
        and (params.seller = 0 or params.seller = t.seller_id)

        {% if env.asan %}
            and t.seller_territory_id in ({{env.asan|join:","}})
        {% endif %}
    group by t.seller_territory_id, t.seller_id
)



select
    tr.title as territory,
    u.last_name ||' '|| u.first_name as seller,
    --
    gr.cnt_property,
    round(f_division(gr.cnt_property_with_auto, gr.cnt_property) * 100) || '%' as property_ratio,
    --
    gr.cnt_travel,
    round(f_division(gr.cnt_travel_with_auto, gr.cnt_travel) * 100) || '%' as travel_ratio,
    --
    gr.seller_id,
    gr.territory_id
from gr
left join reports.territory tr on tr.id = gr.territory_id
left join base_user u on u.id = gr.seller_id

union all

select
    null as territory,
    null as seller,
    --
    sum(gr.cnt_property),
    round(f_division(sum(gr.cnt_property_with_auto), sum(gr.cnt_property)) * 100) || '%' as property_ratio,
    --
    sum(gr.cnt_travel),
    round(f_division(sum(gr.cnt_travel_with_auto), sum(gr.cnt_travel)) * 100) || '%' as travel_ratio,
    --
    0 as seller_id,
    0 as territory_id
from gr

order by territory nulls last, seller nulls last

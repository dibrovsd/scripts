with params as (
    select
        [[env.period.0]] as d_from,
        [[env.period.1]] as d_to,
        [[env.seller]]::integer as seller

        -- current_date - 90 as d_from,
        -- current_date as d_to,
        -- 0 as seller
)

select
    case when q.auto_number != '' then 'С авто' else 'Без авто' end as title,
    count(1) as value
from reports.base_sales t
left join contractor_questionnaire q on q.project_id = t.project_id
                                     and q.document_id = t.id
cross join params
where t.channel_root_id = 7 -- АСАН
    and t.project_id = 3
    and t.d_issue between params.d_from and params.d_to
    and (params.seller = 0 or params.seller = t.seller_id)

    {% if env.channel %}
        and t.channel_territory_id in ({{env.channel|join:","}})
    {% endif %}

group by case when q.auto_number != '' then 'С авто' else 'Без авто' end

with params as (
    select
        [[env.period.0]] as d_from,
        [[env.period.1]] as d_to,
        [[env.seller]]::integer as seller,

        [[get.channel]]::integer as channel_id,
        [[get.seller]]::integer as seller_id,
        [[get.project]]::integer as project_id,
        [[get.only_auto]]::integer as only_auto

        -- current_date - 90 as d_from,
        -- current_date as d_to,
        -- 0 as seller
)

select
    t.n_contract as "Договор",
    ch.title as "Канал продаж",
    u.last_name ||' '|| u.first_name as "Продавец",
    t.ins_person as "Страхователь",
    q.has_auto as "Есть авто",
    q.auto_number as "Гос. номер",
    q.you_driver as "Водите Вы",
    q.phone_is_same as "Контактный номер тот же",
    --
    t.project_id,
    t.id as document_id
from reports.base_sales t
left join contractor_questionnaire q on q.project_id = t.project_id
                                     and q.document_id = t.id
left join base_channel ch on ch.id = t.channel_territory_id
left join base_user u on u.id = t.seller_id
cross join params
where t.channel_root_id = 7 -- АСАН
    and t.project_id in (3, 4)
    and t.d_issue between params.d_from and params.d_to
    and (params.seller = 0 or params.seller = t.seller_id)

    {% if env.channel %}
        and s.channel_territory_id in ({{env.channel|join:", "}})
    {% endif %}

    --
    and (params.channel_id = 0 or t.channel_territory_id = params.channel_id)
    and (params.seller_id = 0 or t.seller_id = params.seller_id)
    and t.project_id = params.project_id
    and (params.only_auto = 0 or q.auto_number != '')

update docflow_document2
    set channel_root_id = root_id,
        channel_sub_id = sub_id,
        channel_territory_id = territory_id
from (
    select
        s.id as src_id,
        ch.root_id,
        ch.sub_id,
        ch.territory_id
    from reports.base_sales s
    left join base_territory t on t.id = s.seller_territory_id
    left join base_channel ch on ch.id = t.channel_id
    where s.product = 'ОСАГО'
) src
where src_id = id;

update docflow_document3
    set channel_root_id = root_id,
        channel_sub_id = sub_id,
        channel_territory_id = territory_id
from (
    select
        s.id as src_id,
        ch.root_id,
        ch.sub_id,
        ch.territory_id
    from reports.base_sales s
    left join base_territory t on t.id = s.seller_territory_id
    left join base_channel ch on ch.id = t.channel_id
    where s.product = 'Недвижимость'
) src
where src_id = id;


update docflow_document4
    set channel_root_id = root_id,
        channel_sub_id = sub_id,
        channel_territory_id = territory_id
from (
    select
        s.id as src_id,
        ch.root_id,
        ch.sub_id,
        ch.territory_id
    from reports.base_sales s
    left join base_territory t on t.id = s.seller_territory_id
    left join base_channel ch on ch.id = t.channel_id
    where s.product = 'ВЗР'
) src
where src_id = id;


update docflow_document11
    set channel_root_id = root_id,
        channel_sub_id = sub_id,
        channel_territory_id = territory_id
from (
    select
        s.id as src_id,
        ch.root_id,
        ch.sub_id,
        ch.territory_id
    from reports.base_sales s
    left join base_territory t on t.id = s.seller_territory_id
    left join base_channel ch on ch.id = t.channel_id
    where s.product = 'Уверенный водитель'
) src
where src_id = id;



update docflow_document12
    set channel_root_id = root_id,
        channel_sub_id = sub_id,
        channel_territory_id = territory_id
from (
    select
        s.id as src_id,
        ch.root_id,
        ch.sub_id,
        ch.territory_id
    from reports.base_sales s
    left join base_territory t on t.id = s.seller_territory_id
    left join base_channel ch on ch.id = t.channel_id
    where s.product = 'Просто КАСКО'
) src
where src_id = id;


update docflow_document9
    set channel_root_id = root_id,
        channel_sub_id = sub_id,
        channel_territory_id = territory_id
from (
    select
        s.id as src_id,
        ch.root_id,
        ch.sub_id,
        ch.territory_id
    from reports.base_sales s
    left join base_territory t on t.id = s.seller_territory_id
    left join base_channel ch on ch.id = t.channel_id
    where s.product = 'Медицина'
) src
where src_id = id;
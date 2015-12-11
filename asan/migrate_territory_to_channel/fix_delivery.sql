update delivery_product
    set channel_id = src_channel
from (
select
    p.id as src_id,
    coalesce(s.channel_territory_id, s.channel_sub_id, s.channel_root_id) as src_channel
from delivery_product p
inner join reports.base_sales s on s.project_id = p.project_id and s.id = p.document_id
) src
where src_id = id
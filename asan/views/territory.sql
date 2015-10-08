create view reports.territory as
select
    tr.id,
    f.title ||' > '|| tr.title as title
from base_territory tr
inner join base_filial f on f.id = tr.filial_id

insert into isbp_targetsqueue (vehicle_number, status, retries, added, baza_id)

select
    t.nomznak,
    1 as status,
    0 as retries,
    current_timestamp as added,
    t."Id" as baza_id
from baza t
WHERE not exists (
    select null from isbp_targetsqueue t1
    where t1.vehicle_number = t.nomznak
)
-- Что докинуть
and (
    sened like '050%' or
    sened like '50%' or
    sened like '055%' or
    sened like '55%' or
    sened like '051%' or
    sened like '51%' or
    sened like '070%' or
    sened like '70%' or
    sened like '071%' or
    sened like '71%'
)

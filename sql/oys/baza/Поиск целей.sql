select count(distinct t.nomznak) from baza t
where not exists (
    select null from base_current t1
    where t1.nomznak = t.nomznak
)
and (nomznak like '10%' or nomznak like '90%' or nomznak like '99%')

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

-- Заполнить регион по префиксу номера

update crm_auto
set region = region_src
from (
    select
        t.id as id_src,
        t1.region as region_src
    from (
        select t.id,
        t.vehicle_number,
        unnest(regexp_matches(t.vehicle_number, '^\d{1,2}')) as number_prefix
        from crm_auto t
        where t.region is null
    ) t
    join crm_autonumprefix t1 on t.number_prefix::integer = t1."prefix"
) src
where id_src = id

drop view reports.base_sales;

create or replace view reports.base_sales as

select
    s.*,
    -- Территория на момент продажи
    coalesce((
        select terr_log.territory_id
        from base_userlog terr_log
        where terr_log.user_id = s.seller_id
          and (terr_log.d_create at time zone 'Asia/Baku') <= s.d_issue
        order by terr_log.d_create desc
        limit 1),
        s.current_territory_id
    ) as seller_territory_id
from (
    -- ОСАГО
    select t.id,
           t.seller_id,
           t.inscompany_id,
           'ОСАГО' as product,
           t.bso_id,
           t.s7_id,
           t.s_premium,
           t.s_discount,
           t.seller_territory_id as current_territory_id,
           t.d_issue,
           t.s_comission,
           t.s_comission_no_discount,
           t.ins_person,
           t.ins_phone,
           t.n_contract,
           t.delivery_city_id,
           t.contractor_id,
           2 as project_id,
           -- Доставка
           t.delivery_date,
           t.delivery_time_from,
           t.delivery_time_to,
           t.delivery_region,
           t.delivery_address,
           t.delivery_comments
    from reports.base_osago t

    -- Недвижимость
    union all
    select t.id,
           t.seller_id,
           t.inscompany_id,
           'Недвижимость' as product,
           t.bso_id,
           t.s7_id,
           t.s_premium,
           t.s_discount,
           t.seller_territory_id as current_territory_id,
           t.d_issue,
           t.s_comission,
           t.s_comission as s_comission_no_discount,
           t.ins_person,
           t.ins_phone,
           t.n_contract,
           t.delivery_city_id,
           t.contractor_id,
           3 as project_id,
           -- Доставка
           t.delivery_date,
           t.delivery_time_from,
           t.delivery_time_to,
           t.delivery_region,
           t.delivery_address,
           t.delivery_comments
    from reports.base_realty t

    -- ВЗР
    union all
    select t.id,
           t.seller_id,
           t.inscompany_id,
           'ВЗР' as product,
           null as bso_id,
           t.s7_id,
           t.s_premium,
           0 as s_discount,
           t.seller_territory_id as current_territory_id,
           t.d_issue,
           t.s_comission,
           t.s_comission as s_comission_no_discount,
           t.ins_person,
           t.ins_phone,
           t.n_contract,
           t.delivery_city_id,
           t.contractor_id,
           4 as project_id,
           -- Доставка
           t.delivery_date,
           t.delivery_time_from,
           t.delivery_time_to,
           t.delivery_region,
           t.delivery_address,
           t.delivery_comments
    from reports.base_travel t

    -- Уверенный водитель
    union all
    select t.id,
           t.seller_id,
           t.inscompany_id,
           'Уверенный водитель' as product,
           null as bso_id,
           t.s7_id,
           t.s_premium,
           0 as s_discount,
           t.seller_territory_id as current_territory_id,
           t.d_issue,
           t.s_comission,
           t.s_comission as s_comission_no_discount,
           t.ins_person,
           t.ins_phone,
           t.n_contract,
           t.delivery_city_id,
           t.contractor_id,
           11 as project_id,
           -- Доставка
           t.delivery_date,
           t.delivery_time_from,
           t.delivery_time_to,
           t.delivery_region,
           t.delivery_address,
           t.delivery_comments
    from reports.base_confident_driver t

    -- Просто КАСКО
    union all
    select t.id,
           t.seller_id,
           t.inscompany_id,
           'Просто КАСКО' as product,
           null as bso_id,
           t.s7_id,
           t.s_premium, 0 as s_discount,
           t.seller_territory_id as current_territory_id,
           t.d_issue,
           t.s_comission,
           t.s_comission as s_comission_no_discount,
           t.ins_person,
           t.ins_phone,
           t.n_contract,
           t.delivery_city_id,
           t.contractor_id,
           12 as project_id,
           -- Доставка
           t.delivery_date,
           t.delivery_time_from,
           t.delivery_time_to,
           t.delivery_region,
           t.delivery_address,
           t.delivery_comments
    from reports.base_simple_kasko t

    /**
    * Райдеры
    */

    -- Пятерочка
    union all
    select t.id,
           t.seller_id,
           t.inscompany_id,
           'Пятерочка' as product,
           t.bso_id,
           t.s7_id,
           t.s_premium,
           0 as s_discount,
           t.seller_territory_id as current_territory_id,
           t.d_issue,
           t.s_comission,
           t.s_comission as s_comission_no_discount,
           t.ins_person,
           t.ins_phone,
           t.n_contract,
           t.delivery_city_id,
           t.contractor_id,
           2 as project_id,
           -- Доставка
           t.delivery_date,
           t.delivery_time_from,
           t.delivery_time_to,
           t.delivery_region,
           t.delivery_address,
           t.delivery_comments
    from reports.base_raider_five t

    -- ОСАГО+
    union all
    select t.id,
           t.seller_id,
           t.inscompany_id,
           'ОСАГО+' as product,
           t.bso_id,
           t.s7_id,
           t.s_premium,
           0 as s_discount,
           t.seller_territory_id as current_territory_id,
           t.d_issue,
           t.s_comission,
           t.s_comission as s_comission_no_discount,
           t.ins_person,
           t.ins_phone,
           t.n_contract,
           t.delivery_city_id,
           t.contractor_id,
           2 as project_id,
           -- Доставка
           t.delivery_date,
           t.delivery_time_from,
           t.delivery_time_to,
           t.delivery_region,
           t.delivery_address,
           t.delivery_comments
    from reports.base_raider_osago_plus t

    -- Супер КАСКО
    union all
    select t.id,
           t.seller_id,
           t.inscompany_id,
           'Супер КАСКО' as product,
           t.bso_id,
           t.s7_id,
           t.s_premium,
           0 as s_discount,
           t.seller_territory_id as current_territory_id,
           t.d_issue,
           t.s_comission,
           t.s_comission as s_comission_no_discount,
           t.ins_person,
           t.ins_phone,
           t.n_contract,
           t.delivery_city_id,
           t.contractor_id,
           2 as project_id,
           -- Доставка
           t.delivery_date,
           t.delivery_time_from,
           t.delivery_time_to,
           t.delivery_region,
           t.delivery_address,
           t.delivery_comments
    from reports.base_raider_super_kasko t
) s;

drop view reports.base_sales;

create or replace view reports.base_sales as

    with product_cumul as (
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
               -- Канал продаж
               t.channel_root_id,
               t.channel_sub_id,
               t.channel_territory_id,
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
               -- Канал продаж
               t.channel_root_id,
               t.channel_sub_id,
               t.channel_territory_id,
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
               -- Канал продаж
               t.channel_root_id,
               t.channel_sub_id,
               t.channel_territory_id,
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
               -- Канал продаж
               t.channel_root_id,
               t.channel_sub_id,
               t.channel_territory_id,
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
               -- Канал продаж
               t.channel_root_id,
               t.channel_sub_id,
               t.channel_territory_id,
               -- Доставка
               t.delivery_date,
               t.delivery_time_from,
               t.delivery_time_to,
               t.delivery_region,
               t.delivery_address,
               t.delivery_comments
        from reports.base_simple_kasko t

        -- Медицина
        union all
        select t.id,
               t.seller_id,
               t.inscompany_id,
               'Медицина' as product,
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
               9 as project_id,
               -- Канал продаж
               t.channel_root_id,
               t.channel_sub_id,
               t.channel_territory_id,
               -- Доставка
               t.delivery_date,
               t.delivery_time_from,
               t.delivery_time_to,
               t.delivery_region,
               t.delivery_address,
               t.delivery_comments
        from reports.base_medicine t

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
               -- Канал продаж
               t.channel_root_id,
               t.channel_sub_id,
               t.channel_territory_id,
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
               -- Канал продаж
               t.channel_root_id,
               t.channel_sub_id,
               t.channel_territory_id,
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
               -- Канал продаж
               t.channel_root_id,
               t.channel_sub_id,
               t.channel_territory_id,
               -- Доставка
               t.delivery_date,
               t.delivery_time_from,
               t.delivery_time_to,
               t.delivery_region,
               t.delivery_address,
               t.delivery_comments
        from reports.base_raider_super_kasko t
    ),

    calculated as (
        select
            s.*,

            -- Территория на момент продажи
            (select terr_log.territory_id
             from base_userlog terr_log
             where terr_log.user_id = s.seller_id
               and (terr_log.d_create at time zone 'Asia/Baku') <= s.d_issue
             order by terr_log.d_create desc
             limit 1) as issue_territory_id,

             (select terr_log.territory_id
              from base_userlog terr_log
              where terr_log.user_id = s.seller_id
              order by terr_log.d_create
              limit 1) as issue_territory_id_first,

            -- Суммарная премия за календарный месяц
            sum(s.s_premium) over(partition by to_char(s.d_issue, 'yyyy_mm'),
                                               s.inscompany_id) as s_premium_month

        from product_cumul s
    ),

    calculated1 as (
        select s.*,
            case
                -- AXA Недвижка
                when s.inscompany_id = 11 and s.product = 'Недвижимость' then
                    case
                        when s_premium_month > 7500 then 0.4
                        else 0.35
                    end

                -- AXA ОСАГО
                when s.inscompany_id = 11 and s.product = 'ОСАГО' then
                    case
                        when s_premium_month > 100000 then 0.38
                        when s_premium_month > 75000 then 0.37
                        when s_premium_month > 35000 then 0.355
                        when s_premium_month > 25000 then 0.345
                        when s_premium_month > 15000 then 0.335
                        else 0.3
                    end

                -- АЗСыгорта после октября
                when s.inscompany_id = 3 and s.product = 'ОСАГО' then
                    case
                        when s.d_issue >= to_date('01.10.2015', 'dd.mm.yyyy') then 0.35
                        else 0.3
                    end

                -- Pasha
                when s.inscompany_id = 6 and s.product in ('ОСАГО', 'Недвижимость') then
                    case
                        when s.d_issue >= to_date('01.09.2015', 'dd.mm.yyyy') then 0.3
                        else 0.25
                    end

                -- ATA ОСАГО + Недвижка по нотариусу
                when s.inscompany_id = 8
                     and s.product in ('ОСАГО', 'Недвижимость')
                     and channel_root_id = 15
                    then 0.35

            end as replace_comission
        from calculated s
    )


    select s.id,
        s.seller_id,
        s.inscompany_id,
        s.product,
        s.project_id,
        s.bso_id,
        s.s7_id,
        s.d_issue,
        s.ins_person,
        s.ins_phone,
        s.n_contract,
        s.delivery_city_id,
        s.contractor_id,
        -- Деньги
        s.s_premium,
        s.s_discount,
        case
            when s.replace_comission is not null
                then s.s_premium * s.replace_comission - s.s_discount
            else s.s_comission
        end as s_comission,
        case
            when s.replace_comission is not null
                then s.s_premium * s.replace_comission
            else s.s_comission_no_discount
        end as s_comission_no_discount,
        -- Канал продаж
        s.channel_root_id,
        s.channel_sub_id,
        s.channel_territory_id,
        -- Доставка
        s.delivery_date,
        s.delivery_time_from,
        s.delivery_time_to,
        s.delivery_region,
        s.delivery_address,
        s.delivery_comments,
        -- Если нет актуальной записи лога, ищем самую первую, хоть и устаревшую
        -- а потом текущую (если ни одной записи лога нет)
        coalesce(s.issue_territory_id,
                 s.issue_territory_id_first,
                 s.current_territory_id) as seller_territory_id
    from calculated1 s

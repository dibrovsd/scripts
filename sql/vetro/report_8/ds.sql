with params as (
    select
        [[env.city_auto_host]]::integer as city_auto_host,
        [[env.direction_stoa]]::integer as direction_stoa,
        [[env.stoa_company]]::integer as stoa_company,
        [[env.inscompany]]::integer as inscompany,

        {% if env.period %}
            [[env.period.0]]::date as d_from,
            [[env.period.1]]::date as d_to
        {% else %}
            null::date as d_from,
            null::date as d_to
        {% endif %}

        -- 0::integer as city_auto_host,
        -- 0::integer as direction_stoa,
        -- 0::integer as stoa_company,
        -- 0::integer as inscompany,
        -- to_date('01.07.2015', 'dd.mm.yyyy')::date as d_from,
        -- to_date('01.09.2015', 'dd.mm.yyyy')::date as d_to
)

select
    d.id,
    d.d_create as "Зарегистрировано",
    d.event_create as "Отправлено",
    d.state as "Этап",
    d.direction_num as "Номер направления",
    d.city as "Город пребывания ТС",
    d.stoa as "СТОА",
    d.inscompany as "СК",
    d.direction_get_date as "Дата получения направления",
    d.auto_mark as "Марка",
    d.auto_model as "Модель",
    d.auto_createyear as "Год выпуска",
    d.replace_glass_glass_type as "Тип стекла",
    d.gfr_code_euro as "Еврокод",
    d.gfr_code_original as "Оригинальный код",
    d.gfr_code_manufacturer as "Производитель",
    d.s_repair_glass as "Стоимость стекла",
    d.s_repair_work as "Стоимость работ",
    d.s_repair_all as "Итого по заказ-наряду"
from reports.v_document d
cross join params
where (params.city_auto_host = 0 or d.city_auto_host_id = params.city_auto_host)
  and (params.direction_stoa = 0 or d.stoa_id = params.direction_stoa)
  and (params.stoa_company = 0 or d.stoa_company_id = params.stoa_company)
  and (params.inscompany = 0 or d.inscompany_id = params.inscompany)
  and d.d_create between params.d_from and params.d_to

  {% if not get.full %}
  limit 100
  {% endif %}

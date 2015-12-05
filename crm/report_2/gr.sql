with call_task as (
    select
        count(case when t.status = 0 then 1 end) as "В ожидании",
        count(case when t.status = 12 then 1 end) as "Ожидание документов",
        count(case when t.status = 8 then 1 end) as "Сам свяжется",
        count(case when t.status = 11 then 1 end) as "Перезвонить",
        count(case when t.status = 17 then 1 end) as "Направлен в АСАН",
        count(case when t.status = 14 then 1 end) as "Отправлен курьер",
        count(case when t.status = 16 then 1 end) as "Перезвонить. Не явился в АСАН",

        count(case when t.status = 2 then 1 end) as "Уже застрахован",
        count(case when t.status = 3 then 1 end) as "Продал авто, есть новый",
        count(case when t.status = 4 then 1 end) as "Продал авто, нет нового",
        count(case when t.status = 5 then 1 end) as "Дорого",
        count(case when t.status in (6, 7, 9, 10, 13, 15, 18) then 1 end) as "Прочее",

        count(case when t.status = 1 then 1 end) as "Продажа",
        count(1) as "Создано"
    from ({{datasets.base.sql}}) t
)

select
    "Создано",
    "В ожидании",
    "Ожидание документов",
    "Сам свяжется",
    "Перезвонить",
    "Направлен в АСАН",
    "Отправлен курьер",
    "Перезвонить. Не явился в АСАН",
    "Уже застрахован",

    "Продал авто, есть новый",
    "Продал авто, нет нового",
    "Дорого",
    "Прочее",

    "Продажа",
    round(f_division("Продажа"::numeric, "Создано") * 100) || '%' as "Проникновение"

from call_task

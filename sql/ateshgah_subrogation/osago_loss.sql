with args as (
    select
    {% if get.date|length == 4 %}
        convert(datetime, [[get.date]]) as date_from,
        dateadd(ss, -1, dateadd(dd, 365, convert(datetime, [[get.date]]))) as date_to
    {% elif get.date|length == 7 %}
        convert(datetime, ([[get.date]] + '-01')) as date_from,
        dateadd(ss, -1, dateadd(mm, 1, convert(datetime, ([[get.date]] + '-01')))) as date_to
    {% else %}
        convert(datetime, [[get.date]]) as date_from,
        dateadd(ss, -1, dateadd(dd, 1, convert(datetime, [[get.date]]))) as date_to
    {% endif %}
)

select
    row_number() over(order by cl.application_number) as "№",
    case 
        when ua.user_guid = '7EA38D7A-AFCA-4A96-BD50-562E17BBA334'
            then 'subrogation'
        when te.forwarded_service_date is not null 
            then 'stoa'
        when te.last_act_avto_harm > 0
            then 'money'
    end as osago_type,
    ---
    cl.draft_number as "Номер страх события", 
    cl.application_number as "Номер убытка", 
    case
        when cl.draft_number = cl.application_number then 'Main'
        else 'added'
    end as "Тип убытка",
    cl.Create_date as "Дата создания убытка",
    u.fullname as "Регистратор убытка",
    cl.insurance_type as "Вид страхования",
    sh1.status_oid as "id статуса",
    sh1.status_date as "Дата статуса",
    s.Description as "Название статуса",
    ua.fullname as "Аварком/офис",
    te.forwarded_service_date as "Дата отправки на СТОА",
    st.name as "Название СТОА",
    te.last_harm_act_sum as "Оценка ущерба",
    (
        select count(1)
        from doument_link dl
        where dl.claim_guid = cl.claim_guid
         and dl.receive_date is not null
    ) as "Получено документов",
    1 as "Дата",
    1 as "Сумма"
from claims as cl
-- last transaction
left join (
    select sh.*,
        row_number() over(partition by sh.claim_guid order by sh.status_date desc) as rn
    from status_history sh
) sh on sh.claim_guid = cl.claim_guid and sh.rn = 1
left join avarkom as ak ON ak.claim_guid = cl.claim_guid
left join users as ua ON ak.user_guid = ua.user_guid
left join users as u ON cl.created_by = u.user_guid
--
left join status as s on s.status_oid = sh1.status_oid
left join technical_expert as te on te.claim_guid = cl.claim_guid
left join st AS st ON st.st_oid = cl.forwarded_service
left join juridical_ekspert AS je ON je.claim_guid = cl.claim_guid
--
cross join args 
where cl.draft_number = cl.application_number
    and cl.create_date >= cast('2014-03-01' as date)
    and cl.insurance_type = 'Osaqo'
    and te.last_act_avto_harm > 0
    --
    and te.last_harm_act_date between args.date_from and args.date_to

select
    to_char(sms.created at time zone 'Asia/Baku', 'dd.mm.yyyy hh24:mi') as "Дата отправки",
    case
        when sms.status = 1 then 'Ожидает доставки'
        when sms.status = 2 then 'Доставлено'
        when sms.status = 3 then 'Ошибка доставки'
        when sms.status = 4 then 'Удалено из очереди доставки'
        when sms.status = 5 then 'Системная ошибка'
    end  as "Статус отправки",
    sms.message as "Текст сообщения"
from lib_smstaskmessage sms
where sms.msisdn = [[env.phone]]
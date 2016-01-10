from lib.sms_ata import AtaSmsService
from contractor.models import ContractorPerson

# from docflow.models import Document2
# d = Document2.objects.get(pk=10466)
# print d.contractor_id


message = u'Dunya Azerbaycanlilarinin hemreylik gunu ve Yeni iliniz mubarek! ' \
          u'Sizin Odlar Yurdu Sigorta Brokeriniz. Tel.:*0707'
client = AtaSmsService()
sended_tasks = []


def send(phone_list):
    task_id = client.submit_bulk(task_type='happy_new_year',
                                 message=message,
                                 recipient_isdns=phone_list)
    sended_tasks.append(task_id)
    print task_id


qs = ContractorPerson.objects.filter(phone__isnull=False) \
    .extra(where=[u'''
        not exists (
            select null
            from lib_smstaskmessage extra_msg
            inner join lib_smstask extra_task on extra_task.id = extra_msg.task_id
            where extra_msg.msisdn = contractor_contractorperson.phone
              and extra_task.task_type = 'happy_new_year'
        )'''])
qs = qs.distinct('phone')

# qs = qs.filter(contractor_id=9988)  # Отладка
# qs = qs.filter(phone='994502142590')  # Отладка
# for p in qs:
#     print p

sended_msgs = set()
for task_id in sended_tasks:
    res = client.get_detailed_report(task_id)
    for msg in res['messages']:
        sended_msgs.add(msg['msisdn'])

phone_to_send = set([person.phone for person in qs])

not_send = phone_to_send - sended_msgs

# for m in sended_msgs:
#     if m == '994502142590':
#         print m

phone_list = []
for person in qs:
    if person.phone in sended_msgs:
        continue

    if len(person.phone) > 12:
        continue

    phone_list.append(person.phone)

    # Накопили достаточно
    # Отправим и сбросим буфер
    if len(phone_list) >= 500:
        send(phone_list)
        phone_list = []

if phone_list:
    send(phone_list)

# Проверим
# client.get_detailed_report(task_id)
for task_id in sended_tasks:
    print client.get_short_report(task_id)

for ph in phone_list:
    if len(ph) > 12:
        print ph

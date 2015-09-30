u"""
Копировать данные по ОСАГО из таблицы Excel для пролонгации

update osago_old_data set auto_num = replace(auto_num, ' ', '');
update osago_old_data set n_contract = replace(n_contract, '-', '');

update osago_old_data set client_id = client_id_src
from (
    select t.id as id_src, a.client_id as client_id_src from osago_old_data t
    join crm_auto a on a.vehicle_number = t.auto_num
) t
where id_src = id;
"""

from crm import models
from crm.tasks import refresh_auto_contracts
from django.db import transaction


qs = models.OsagoOldData.objects.order_by('id').filter(id__gt=3600)
cnt = qs.count()

i = 0
for obj in qs:
    i += 1
    with transaction.atomic():
        print u'%s из %s' % (i, cnt), obj.id
        try:
            # Договор есть в CRM, просто ставим ему канал продаж и выходим
            contract = models.InsContractAuto.objects.get(contract_number=obj.n_contract)
            contract.channel = 'asan'
            contract.save(update_fields=['channel'])

            # Не забываем про клиента
            client = contract.auto.client
            try:
                client_person = client.person
            except models.ClientPerson.DoesNotExist:
                client_person = models.ClientPerson(client=client)

            client_person.last_name = obj.last_name
            client_person.first_name = obj.name
            client_person.middle_name = obj.middle_name
            client_person.save()

            if not models.ClientPhone.objects.filter(client=client, phone=obj.phone).exists():
                phone = models.ClientPhone(client=client, phone=obj.phone)
                phone.save()

            continue

        except models.InsContractAuto.DoesNotExist:
            pass

        # Договор не найден
        try:
            auto = models.Auto.objects.get(vehicle_number=obj.auto_num)

        except models.Auto.DoesNotExist:
            # Если машины нет, то создаем клиента и машину в нем
            # (все равно без машины мы клиента не определим)
            client = models.Client(client_type='person')
            client.save()

            client_person = models.ClientPerson(client=client,
                                                last_name=obj.last_name,
                                                first_name=obj.name,
                                                middle_name=obj.middle_name)
            client_person.save()

            auto = models.Auto(client=client,
                               vehicle_number=obj.auto_num,
                               status='active')
            auto.save()

        # Тянем из Бюро
        try:
            refresh_auto_contracts(auto.id)

            try:
                # Ищем наш договор (мы уже его затянули)
                # и ставим канал продаж
                contract = auto.inscontracts.get(contract_number=obj.n_contract)
                contract.channel = 'asan'
                contract.save(update_fields=['channel'])

            except models.InsContractAuto.DoesNotExist:
                print u'Не нашел договора после обновления', client.pk, obj.n_contract, obj.auto_num

        except Exception as e:
            print e

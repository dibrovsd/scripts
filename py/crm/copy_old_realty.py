u"""
Копировать данные по Недвижимости из таблицы Excel для пролонгации

update tmp_realty set insurer = trim(insurer);
update tmp_realty set insurer = replace(insurer, '  ', ' ');

update tmp_realty set n_contract = replace(n_contract, '-', '');
update tmp_realty set l_name = substr(insurer, 1, strpos(insurer, ' ')-1) where insurer like '% %';

update tmp_realty set f_name = substr(insurer, length(l_name)+2) where insurer like '% %';
update tmp_realty set f_name = substr(f_name, 1, strpos(f_name, ' ')-1) where f_name like '% %';

update tmp_realty set m_name = substr(insurer, length(l_name) + 2 + length(f_name) + 1) where f_name like '% %';
"""

from crm import models
from django.db import transaction


qs = models.TmpRealty.objects.order_by('id').filter(id__gt=5)
cnt = qs.count()

i = 0
for obj in qs:
    i += 1
    with transaction.atomic():
        if models.InsContractRealty.objects.filter(contract_number=obj.n_contract).exists():
            continue

        client = models.Client(client_type='person')
        client.save()

        client_person = models.ClientPerson(client=client,
                                            last_name=obj.l_name,
                                            first_name=obj.f_name,
                                            middle_name=obj.m_name)
        client_person.save()

        phone = models.ClientPhone(client=client, phone=obj.phone)
        phone.save()

        realty = models.Realty(address=obj.address or '---',
                              client=client)
        realty.save()

        contract = models.InsContractRealty(realty=realty,
                                            insurance_company=obj.company,
                                            contract_number=obj.n_contract,
                                            d_start=obj.d_start,
                                            d_end=obj.d_end,
                                            channel='asan')
        contract.save()

        print u'%s из %s' % (i, cnt), obj.id, client.pk

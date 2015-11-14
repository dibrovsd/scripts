from crm import models
from crm.tasks import refresh_auto_contracts
from django.db import transaction

qs = models.GibddData.objects.extra(where=[u'''
    not exists (select null from crm_auto
                where crm_auto.vehicle_number = crm_gibdd_data.nomznak)
'''])

# qs = qs[:5]

cnt = qs.count()

i = 0
for a in qs.iterator():
    i += 1

    with transaction.atomic():
        cl = models.Client(client_type='person',
                           comments=a.adr)
        cl.save()

        person = models.ClientPerson(client=cl,
                                     last_name=a.vladel,
                                     first_name='-',
                                     middle_name='-',)
        person.save()

        if a.sened:
            ph = models.ClientPhone(client=cl, phone=a.sened)
        else:
            ph = models.ClientPhone(client=cl, phone=a.sovlad)

        ph.save()

        try:
            create_year = int(a.gvip)
        except:
            create_year = None

        auto = models.Auto(client=cl,
                           vehicle_number=a.nomznak,
                           brand=a.marka,
                           n_body=a.nbody,
                           n_engine=a.ndvig,
                           create_year=create_year)
        auto.save()

        print i, cnt, a.nomznak
    refresh_auto_contracts.delay(auto.id)


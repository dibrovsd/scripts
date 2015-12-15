from crm.utils import fix_phone
from crm.models import ClientPhone
from django.db import transaction

with transaction.atomic():
    i = 0
    for p in ClientPhone.objects.iterator():
        new_phone = fix_phone(p.phone)
        if new_phone != p.phone:
            p.phone = fix_phone(p.phone)
            p.save(update_fields=['phone'], force_update=True)

        i += 1
        if i % 1000 == 0:
            print i

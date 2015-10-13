from docflow.models import Event1, EventAccess1, DocumentEvent1
from django.db import transaction

qs = Event1.objects.filter(pk__in=[50, 51, 52, 54, 55, 56, ])
qs = Event1.objects.filter(pk__in=[52, 54, 55, 56])

# 53 был не корректировочным
with transaction.atomic():
    for e in qs:

        # Создаем новый переход
        new_e = Event1(
            title=e.title,
            group=e.group,
            n_order=e.n_order,
            state_from=e.state_from,
            state_to=e.state_to,
            change_responsible_type=e.change_responsible_type,
        )
        new_e.save()

        new_e.role_recipients = e.role_recipients.all()

        for ea in e.access.all():
            new_ea = EventAccess1(event=new_e, role=ea.role, responsed_only=ea.responsed_only)
            new_ea.save()

        DocumentEvent1.objects.filter(event=e).update(event=new_e)

        print u'%s -> %s' % (e.id, new_e.id)

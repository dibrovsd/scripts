from docflow.models import Document1, DocumentEvent1
from datetime import date
from base.models import User
from docflow.utils import get_project_manager
from django.db import transaction


qs = Document1.objects.filter(last_event__event__id=9,
                              last_event__d_create__gte=date.today())

user = User.objects.get(id=8)
project_manager = get_project_manager(user=user, project_id=1)

# qs = qs[:10]

with transaction.atomic():
    for d in qs:
        if 1 in d.opened_tasks or {2, 3, } & d.opened_tasks:
            print d.pk

            ev = d.events.exclude(pk=d.last_event.pk).latest('d_create')
            responsible = ev.user_responsible

            d.do_send(user_creator=user,
                      project_manager=project_manager,
                      event_id=98,
                      user_responsible=responsible)


# Удалить лишние события
qs = Document1.objects.filter(last_event__event__id=98).order_by('id')
# qs = qs[:3]

with transaction.atomic():
    for d in qs:
        print d.id

        d.last_event = d.events.exclude(event_id__in=(9, 98)).latest('d_create')
        d.save(update_fields=['last_event'])

        d.events.filter(d_create__gt=d.last_event.d_create).delete()

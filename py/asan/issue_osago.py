u"""
Если мы потеряли выданный из интеграции договор,
то этот скрипт проставляет номер договора вручную в документ
и отправляет его на "Вступил в силу"
"""

from django.core.cache import cache
from django.utils import timezone
from docflow.models import Document2 as Document
from docflow.utils import get_project_manager
from base.models import User

project_id = 2
n_contract = u'PAZ1503976917'
user_id = 8
document_id = 10810
bso_id = None

user = User.objects.get(pk=user_id)

document = Document.objects.get(pk=document_id)
pm = get_project_manager(user=user, project_id=project_id)

document.n_contract = n_contract
document.d_issue = timezone.now()
document.save(update_fields=('n_contract', 'd_issue', ))

if bso_id:
    document.bso_id = bso_id
    document.save(update_fields=['bso_id'])

# Пометим бланки как выпущеные
document.bso.set_contract(project_id, document.pk)
if document.s7:
    document.s7.set_contract(project_id, document.pk)

document.do_refresh_tags()

# Переход на "Договор вступил в силу" при выдаче полиса
document.do_send(project_manager=pm,
                 event_id=2,
                 user_creator=user,
                 message='Automatic event')

cache.delete('motivation:%d' % user.id)

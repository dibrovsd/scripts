u"""
Если мы потеряли выданный из интеграции договор,
то этот скрипт проставляет номер договора вручную в документ
и отправляет его на "Вступил в силу"
"""

from django.core.cache import cache
from django.utils import timezone
from docflow.models import Document2, Document3
from docflow.utils import get_project_manager
from base.models import User


# Менять тут
document_id = 3889
n_contract = u'RAT1504044943'

# Константы
project_id = 3
user = User.objects.get(pk=42)  # Дибров

document = Document3.objects.get(pk=document_id)
pm = get_project_manager(user=user, project_id=project_id)

document.n_contract = n_contract
document.d_issue = timezone.now()
document.save(update_fields=('n_contract', 'd_issue', ))


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

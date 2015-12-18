u"""
Если мы потеряли выданный из интеграции договор,
то этот скрипт проставляет номер договора вручную в документ
и отправляет его на "Вступил в силу"
"""

from django.utils import timezone
from docflow.models import Document2, Document3
from docflow.utils import get_project_manager


# Менять тут
project_id = 3
document_id = 16897
n_contract = u'PAX1504233870'

# Тут менять не нужно

if project_id == 2:
    document = Document2.objects.get(pk=document_id)
elif project_id == 3:
    document = Document3.objects.get(pk=document_id)

user = document.user_creator
channel = user.channel
pm = get_project_manager(user=user, project_id=project_id)

document.n_contract = n_contract
document.d_issue = timezone.now()
document.channel_root = channel.root
document.channel_sub = channel.sub
document.channel_territory = channel.territory

document.save()

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

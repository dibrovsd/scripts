u"""
1) По задачам, по которым мы не сделали продажу (а договор оканчивался)
проверим наличие договоров из Бюро.

2) Если у человека так и не появилось действующего договора,
Снова поставить задачу на Фатиму.
Раз он не застрахован все еще, то может быть сейчас согласится
"""

from crm.tasks import refresh_contracts
from crm.models import CallTask

# - Ожидание документов
# - Отказ - Слишком дорого
# - Отказ - Нет нужной СК
# - Отказ - Сам свяжется
# - Не дозвонились
# - Просрочена
# - Перезвонить
qs = CallTask.objects.filter(status__in=[12, 5, 6, 8, 9, 10, 11])
qs = qs.order_by('id')[:100]

for task in qs:
    if task.comment and task.comment.startswith(u'processed'):
        continue

    res = refresh_contracts(task.pk)

    if res['status'] == 'unchanged':
        task.status = 0
        task.responsible_id = 45  # Fatima Huseynova
        print u'create', task.pk

    task.comment = u'processed %s' % task.comment
    task.save()

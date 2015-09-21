u"""
Обновить статус договоров из Бюро
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

with open('not_find_contracts.txt', 'w') as log:
    i = 0
    for task in qs:
        try:
            print u'process', i, task.pk
            res = refresh_contracts(task.pk)
            if res['status'] == 'unchanged':
                log.write(unicode(task.pk) + u'\n')
        except:
            pass

        i += 1

u"""
1) По задачам, по которым мы не сделали продажу (а договор оканчивался)
проверим наличие договоров из Бюро.

2) Если у человека так и не появилось действующего договора,
Снова поставить задачу на Фатиму.
Раз он не застрахован все еще, то может быть сейчас согласится
"""

from crm.tasks import refresh_contracts
from crm.models import CallTask
from datetime import date

# - Ожидание документов
# - Отказ - Слишком дорого
# - Отказ - Нет нужной СК
# - Отказ - Сам свяжется
# - Не дозвонились
# - Просрочена
# - Перезвонить
qs = CallTask.objects.filter(status__in=[12, 5, 6, 8, 9, 10, 11]) \
                     .exclude(comment__startswith=u'processed')
qs = qs.order_by('id')
cnt = qs.count()
i = 0

for task in qs.iterator():
    res = refresh_contracts(task.pk)

    if task.contract.d_end < date.today():
        task.status = 0
        task.responsible_id = 45  # Fatima Huseynova

    task.comment = u'processed #1091 %s' % task.comment
    task.save()

    print u'Обработано %s из %s' % (i, cnt)

    i += 1

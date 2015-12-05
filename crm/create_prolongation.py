from datetime import date, datetime
from dateutil.relativedelta import relativedelta

from crm.task_distribution import create_tasks_from_obj, distribute_count
from base.models import User
from crm.models import InsContractAuto, Auto, InsContractRealty, Realty, CallTask

d_from = date(2015, 12, 1)
d_to = date(2015, 12, 31)
today_plus_25 = date.today() + relativedelta(days=25)
today_plus_5 = date.today() + relativedelta(days=5)
today_minus_30 = date.today() - relativedelta(days=30)
users_qs = User.objects.filter(is_active=True, roles__name='renew_asan')

# TEST
# distribute_count(cnt=100, users=users_qs, with_performance=True)
# distribute_count(cnt=100, users=users_qs, with_performance=False)

# now_minus_1 = datetime.now() - relativedelta(hours=1)
# qs = CallTask.objects.filter(d_create__gte=now_minus_1, task_type__in=('renew_osago', 'renew_realty'))
# qs.count()
# /TEST

# ОСАГО
# - Есть договора, которые заканчивается в декабре
# - Нет других действующих договоров
# - Мы ему продавали договор (возможно, не тот, который заканчивается, а другой)
due_contracts = InsContractAuto.objects.filter(d_end__range=[d_from, d_to])
active_contracts = InsContractAuto.objects.filter(d_end__gt=today_plus_25)

auto_qs = Auto.objects.filter(inscontracts__channel__in=['asan', 'call_center'],
                              inscontracts__in=due_contracts) \
                      .exclude(inscontracts__in=active_contracts)
auto_qs = Auto.objects.filter(pk__in=auto_qs).extra(where=[u'''not exists (
              select null from crm_calltask tsk
              where tsk.auto_id = crm_auto.id
            )'''])

create_tasks_from_obj(task_type='renew_osago',
                      users_qs=users_qs,
                      obj_list=list(auto_qs),
                      with_performance=False)

# Недвижимость
realty_contracts_active = InsContractRealty.objects.filter(d_end__gt=today_plus_25)
realty_contracts = InsContractRealty.objects.filter(channel__in=['asan', 'call_center'],
                                                    d_end__range=[d_from, d_to])
realty = Realty.objects.filter(inscontracts__in=realty_contracts) \
                       .exclude(inscontracts__in=realty_contracts_active) \
                       .extra(where=[u'''not exists (
                           select null from crm_calltask tsk
                           where tsk.realty_id = crm_realty.id
                       )'''])
create_tasks_from_obj(task_type='renew_realty',
                      users_qs=users_qs,
                      obj_list=list(realty),
                      with_performance=False)

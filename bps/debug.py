from datetime import datetime

from accounting.models import *
from docflow.models import *
from auth.models import *


# Бух. учет
a = Account.objects.first()
op = OperationType.objects.first()
u = User.objects.first()
now = datetime.utcnow()

for i in range(50000):
    t = Transaction(d_create=now,
                    user=u,
                    d_operation=now,
                    initiator=u,
                    account=a,
                    operation_type=op,
                    value=12,
                    description='Описание операции')
    t.save()

# Документы
d = VetroDocument.objects.first()
d.insurers.create(s_fact=12.3, s_plan=10.4)
d.save()

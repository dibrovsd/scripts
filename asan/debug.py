from docflow.models import Document2
from docflow_projects.utils import *

qs = Document2.objects.filter(d_issue__isnull=False,
                              s_premium__isnull=False,
                              channel_root_id__in=[9, 7, 15])
qs = qs[:1000]

for d in qs:
    create_accounting_transaction(d)

from docflow.models import Document2
from decimal import Decimal

qs = Document2.objects.filter(s_supplements__isnull=True, product_options__isnull=False)

for d in qs:
    s_supplements = Decimal(0.)
    for o in d.product_options.all():
        s_supplements += o.cost

    d.s_supplements = s_supplements
    d.save(update_fields=['s_supplements'])

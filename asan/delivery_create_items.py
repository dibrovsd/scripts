from delivery.utils import create_delivery
from delivery.models import Product
from docflow.models import Document2, Document3, Document4, Document11, Document12

Product.objects.all().delete()

qs = Document2.objects.filter(user_creator__territory_id=9, d_issue__isnull=False)

for d in qs:
    create_delivery(d)

qs = Document3.objects.filter(user_creator__territory_id=9, d_issue__isnull=False)
for d in qs:
    create_delivery(d)

qs = Document4.objects.filter(user_creator__territory_id=9, d_issue__isnull=False)
for d in qs:
    create_delivery(d)


qs = Document11.objects.filter(user_creator__territory_id=9, d_issue__isnull=False)
for d in qs:
    create_delivery(d)

qs = Document12.objects.filter(user_creator__territory_id=9, d_issue__isnull=False)
for d in qs:
    create_delivery(d)

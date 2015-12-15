from delivery.utils import create_delivery
from docflow.models import Document2, Document3, Document4, Document11, Document12

# from delivery.models import Product
# Product.objects.all().delete()

qs = Document2.objects.filter(user_creator__channel__root_id=9,
                              d_issue__isnull=False,
                              canceled=False) \
    .extra(where=[u''' not exists (
        select null from delivery_product delivery
        where delivery.project_id = 2
        and delivery.document_id = docflow_document2.id
        )'''])
qs.count()
for d in qs:
    create_delivery(d)

qs = Document3.objects.filter(user_creator__channel__root_id=9,
                              d_issue__isnull=False,
                              canceled=False) \
    .extra(where=[u''' not exists (
        select null from delivery_product delivery
        where delivery.project_id = 3
        and delivery.document_id = docflow_document3.id
        )'''])
qs.count()
for d in qs:
    create_delivery(d)

qs = Document4.objects.filter(user_creator__channel__root_id=9,
                              d_issue__isnull=False,
                              canceled=False) \
    .extra(where=[u''' not exists (
        select null from delivery_product delivery
        where delivery.project_id = 4
        and delivery.document_id = docflow_document4.id
        )'''])
qs.count()
for d in qs:
    create_delivery(d)


qs = Document11.objects.filter(user_creator__channel__root_id=9,
                              d_issue__isnull=False,
                              canceled=False) \
    .extra(where=[u''' not exists (
        select null from delivery_product delivery
        where delivery.project_id = 11
        and delivery.document_id = docflow_document11.id
        )'''])
for d in qs:
    create_delivery(d)

qs = Document12.objects.filter(user_creator__channel__root_id=9,
                              d_issue__isnull=False,
                              canceled=False) \
    .extra(where=[u''' not exists (
        select null from delivery_product delivery
        where delivery.project_id = 12
        and delivery.document_id = docflow_document12.id
        )'''])
for d in qs:
    create_delivery(d)

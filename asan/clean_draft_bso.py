# Сбросить бланки, зарезервированные системой
# Это если бланки все застряли

from django.db.models.loading import get_model

# blank_type
# 1 - ОСАГО
# 2 - S7


Document = get_model('docflow', 'Document1')

qs = Document.objects.filter(contract_pk__isnull=False,
                             blank_status=1,
                             inscompany_id=11,
                             blank_type=1)
for blank in qs:
    print blank, blank.contract, blank.contract.user_creator
    # blank.contract_project = None
    # blank.contract_pk = None
    # blank.save(update_fields=('contract_project', 'contract_pk',))
    # blank.do_refresh_tags()

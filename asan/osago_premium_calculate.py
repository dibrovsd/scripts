from datetime import date
from docflow.models import Document2
from docflow_projects.utils import get_premium_discounted


date_max = date(2015, 7, 8)

for d in Document2.objects.filter(d_issue__lt=date_max).order_by('d_issue'):
    old = d.s_premium_discounted

    s_premium = d.s_premium_base * d.bonus_malus
    new = get_premium_discounted(s_premium=s_premium,
                                 discount_percent=d.discount_percent,
                                 document=d)

    d.s_premium_discounted = new
    d.save(update_fields=['s_premium_discounted'])

    if new and old and round(new, 2) != round(old, 2):
        print d.pk, round(old, 2), round(new, 2)

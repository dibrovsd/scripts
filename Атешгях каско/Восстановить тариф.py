from django.db.models.signals import post_save
from docflow.projects.modules_2.signals import document_post_save
from docflow.models import Document2

post_save.disconnect(document_post_save, sender=Document2)


for d in Document2.objects.all():
    discount_of_agent = float(d.discount_of_agent) / 100 if d.discount_of_agent else float(0)
    discount_of_tarif = float(d.discount_of_tarif) if d.discount_of_tarif else float(0)
    calculated_restored = (float(d.s_premium_for_pay) + discount_of_tarif * discount_of_agent + discount_of_tarif) / (1 - discount_of_agent)

    d.calculated_prize_freeze = round(calculated_restored, 2)
    d.save(update_fields=('calculated_prize_freeze',))
    
    print 'success', d.id


# скидка = (расчет - ск_тариф) * ск_агентская + ск_тариф
# итоговый = расчет - скидка

# итоговый = расчет - ((расчет - ск_тариф) * ск_агентская + ск_тариф)
# итоговый = расчет - (расчет - ск_тариф) * ск_агентская - ск_тариф
# итоговый = расчет - расчет * ск_агентская - ск_тариф * ск_агентская  - ск_тариф
# итоговый = расчет(1 - ск_агентская) - ск_тариф * ск_агентская - ск_тариф
# итоговый + ск_тариф * ск_агентская + ск_тариф = расчет(1 - ск_агентская)
# (итоговый + ск_тариф * ск_агентская + ск_тариф) / (1 - ск_агентская) = расчет
# ---
# расчет = (итоговый + ск_тариф * ск_агентская + ск_тариф) / (1 - ск_агентская)
from docflow_projects.utils import email_nofify
from docflow.models import Document1
from mailer.models import Message2
from mailer.functions import get_attachments, decode_value
import email


d = Document1.objects.get(pk=2884)
email_nofify(d, 'event_to_generation_invoice_deductible')

######### Почтовик #########

# Работаем с вложениями руками
m = Message2.objects.filter(subject__contains='368620/15')
eml = email.message_from_string(m.eml.read())

for part in eml.walk():
    content_type = part.get_content_type()
    print content_type

# Проверяем корректность вложений всех писем
for m in Message2.objects.all():
    print m.id
    eml = email.message_from_string(m.eml.read())
    attachments = get_attachments(eml)
    for attachment in attachments:
        print u'Получилось', attachment['filename']


# Ковыряем
m = Message2.objects.get(pk=8)

qs = Message2.objects.all()
for m in qs:
    eml = email.message_from_string(m.eml.read())
    for part in eml.walk():
        content_type = part.get_content_type()

        if 'application' in content_type:
            filename = part.get_filename()
            try:
                filename = decode_value(filename)
            except:
                pass

            if type(filename) != unicode:
                filename = filename.decode('utf-8')

            print filename, type(filename)

#
qs = Message2.objects.filter(pk__in=[63, 58, 55, 47])
for m in qs:
    print m.document_id

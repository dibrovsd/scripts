from docflow.models import FieldGroup1
from docflow.utils import get_project_manager

pm = get_project_manager(project_id=1)
fields = {f.name:f for f in pm.document_fields}

type_map = {
    1: 'STRING',
    2: 'FLOAT',
    24: 'INTEGER',
    3: 'DATE',
    9: 'DATETIME',
    4: 'TEXTAREA',
    8: 'BOOL_NULLABLE',
    10: 'BOOL',
    5: 'SELECT_MODEL',
    12: 'SELECT_MODEL_AUTOCOMPLETE',
    16: 'SELECT_CHOICES',
    6: 'SELECT_MULTI',
    17: 'SELECT_MULTI_CHOICES',
    18: 'SELECT_MULTI_AUTOCOMPLETE',
    11: 'HIERARCHY',
    20: 'FORMSET',
    13: 'DATE_RANGE',
    14: 'FLOAT_RANGE',
    22: 'PROPERTY',
    23: 'DECIMAL',
}

for fg in FieldGroup1.objects.order_by('n_order'):
    for f in fg.fields.order_by('n_order_group'):
        fld = fields[f.name]
        print fg, '\t', f.name, '\t', f.title, '\t', type_map[fld.data_type]


######
from docflow.models import FieldGroup16
from docflow.functions import get_project_manager

pm = get_project_manager(project_id=16)
fields = {f.name:f for f in pm.get_fields()}

type_map = {
    1: 'TYPE_STRING',
    2: 'TYPE_FLOAT',
    3: 'TYPE_DATE',
    9: 'TYPE_DATETIME',
    4: 'TYPE_TEXTAREA',
    8: 'TYPE_BOOL_NULLABLE',
    10: 'TYPE_BOOL',
    5: 'TYPE_SELECT_MODEL',
    12: 'TYPE_SELECT_MODEL_AUTOCOMPLETE',
    16: 'TYPE_SELECT_CHOICES',
    6: 'TYPE_SELECT_MULTI',
    17: 'TYPE_SELECT_MULTI_CHOICES',
    18: 'TYPE_SELECT_MULTI_AUTOCOMPLETE',
    11: 'TYPE_HIERARCHY',
    7: 'TYPE_SQL',
    15: 'TYPE_PYTHON',
    19: 'TYPE_TABLE',
    20: 'TYPE_FORMSET',
    13: 'TYPE_DATE_RANGE',
    14: 'TYPE_FLOAT_RANGE',
    21: 'TYPE_CONTRACTOR',
    22: 'TYPE_PROPERTY',
    23: 'TYPE_DECIMAL',
}

for fg in FieldGroup16.objects.order_by('n_order'):
    for f in fg.fields.order_by('n_order_group'):
        if f.name not in fields:
            continue
        
        fld = fields[f.name]
        print fg, '\t', f.name, '\t', f.title, '\t', type_map[fld.data_type]
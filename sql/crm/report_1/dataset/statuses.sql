select
    lib.id,
    tsk.status
from reports.libs lib
where lib.lib_name = 'calltask_status'

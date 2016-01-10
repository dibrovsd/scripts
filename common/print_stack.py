import traceback
from pprint import pprint
exception_type, exception_value, tb = record.exc_info

print '----------- print_tb  ---------'
traceback.print_tb(tb)

print '----------- print_exception  ---------'
traceback.print_exception(exception_type, exception_value, tb)

print '----------- print_exc  ---------'
traceback.print_exc()

print '----------- format_exc  ---------'
pprint(traceback.format_exc())

print '----------- extract_tb  ---------'
pprint(traceback.extract_tb(tb))

print '----------- extract_stack  ---------'
pprint(traceback.extract_stack())

print '----------- format_exception_only  ---------'
pprint(traceback.format_exception_only(exception_type, exception_value))

print '----------- format_exception  ---------'
pprint(traceback.format_exception(exception_type, exception_value, tb))

print '----------- format_tb  ---------'
pprint(traceback.format_tb(tb))

print '----------- format_stack  ---------'
pprint(traceback.format_stack())

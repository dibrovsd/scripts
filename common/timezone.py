from django.db import connections
from dateutil import tz
from datetime import date, timedelta
from pytz import timezone
from base.models import AsteriskCall

conn = connections['reports_asterisk']
curs = conn.cursor()

curs.execute("SELECT * FROM mysql.time_zone_name")
curs.execute("SET time_zone = 'Asia/Baku'")
curs.execute("SET time_zone = 'UTC'")

curs.execute('''
select 
    calldate,
    uniqueid
from cdr
where 
uniqueid = '1431956702.16047'
order by calldate
''')
calldate = curs.fetchall()[0][0]
calldate
for r in curs.fetchall():
    print r

# datetime.datetime(2015, 5, 18, 18, 45, 2, tzinfo=<UTC>)

tz_baku = tz.gettz('Asia/Baku')
tz_utc = tz.gettz('UTC')

calldate = calldate.replace(tzinfo=None)
calldate = calldate.replace(tzinfo=tz_baku)
calldate = calldate.replace(tzinfo=tz_utc)

calldate = calldate.replace(tzinfo=None)
calldate = calldate.replace(tzinfo=timezone('UTC'))
calldate = calldate.replace(tzinfo=timezone('Asia/Baku'))

# 2
timezone('Asia/Baku').localize(calldate)

# Исправить TZ на другую
calldate = calldate.replace(tzinfo=tz_baku)


calldate.strftime("%Y-%m-%d %H:%M:%S %Z%z")

o = AsteriskCall(calldate=calldate, 
                 src='TEST', 
                 des='TEST', 
                 duration=0, 
                 disposition='TEST')
o.save(force_insert=True)
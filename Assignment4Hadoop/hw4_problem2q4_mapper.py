#!/usr/bin/env python
'''
mapper
Query4
get count of unique URLs by country by hour for a  specified time range [t1-t2]
where t1 = 09/13/2019 5PM UTC and
t2 = 09/14/2019 9AM UTC
Both range ends are inclusive
'''

#Loi Cheng
#9/xx/2019

import sys
from datetime import datetime

# input must come from STDIN (standard input)
for line in sys.stdin:

    columns = line.split(',')
    dateHour = columns[1].split(':',1)[0].replace('T',':')
    # 2019-09-14:23:55:14.880Z
    country = columns[4]
    url = columns[2]

    t1 = datetime.strptime( '2019-09-13:17:00:00' , '%Y-%m-%d:%H:%M:%S')
    t2 = datetime.strptime( '2019-09-14:09:00:00' , '%Y-%m-%d:%H:%M:%S')
    dateHourDateTime = datetime.strptime( dateHour, '%Y-%m-%d:%H' )

    if ( dateHourDateTime >= t1 and dateHourDateTime <= t2 ):
        # output key and value to console, must be tab delimited, must be strings
        print( '%s\t%s' % (dateHour+'::'+country+'::'+url, 1)  )
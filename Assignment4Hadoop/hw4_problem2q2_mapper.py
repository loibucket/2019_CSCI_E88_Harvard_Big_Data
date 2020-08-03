#!/usr/bin/env python
'''
mapper
Query2: get count of unique visitors per URL per hour
'''

#Loi Cheng
#9/xx/2019

import sys

# input must come from STDIN (standard input)
for line in sys.stdin:

    columns = line.split(',')
    dateHour = columns[1].split(':',1)[0].replace('T',':')
    url = columns[2]
    user = columns[3]

    # output key and value to console, must be tab delimited, must be strings
    print( '%s\t%s' % (dateHour+'::'+url+'::'+user, 1)  )
#!/usr/bin/env python
'''
mapper
Query1: get count of unique URLs per hour
'''

#Loi Cheng
#9/xx/2019

import sys

# input must come from STDIN (standard input)
for line in sys.stdin:

    columns = line.split(',',4)
    dateHour = columns[1].split(':',1)[0].replace('T',':')
    url = columns[2]

    # output key and value to console, must be tab delimited, must be strings
    print( '%s\t%s' % (dateHour+'::'+url, 1)  )
#!/usr/bin/env python
'''
mapper
Query3: get count of unique (by UUID) events/clicks per URL per hour (one unique click == one log line in our test log files, since we assume they all have unique UUIDs)
'''

#Loi Cheng
#9/xx/2019

import sys

# input must come from STDIN (standard input)
for line in sys.stdin:

    columns = line.split(',')
    dateHour = columns[1].split(':',1)[0].replace('T',':')
    url = columns[2]

    # output key and value to console, must be tab delimited, must be strings
    print( '%s\t%s' % (dateHour+':'+url, 1)  )
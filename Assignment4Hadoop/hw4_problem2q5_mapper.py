#!/usr/bin/env python
'''
mapper
Query5
get top 5 URLs by avg daily TTFB, per each day in the input data set; consider "top" URLs to be those that have the smallest avg TTFB
'''

#Loi Cheng
#9/xx/2019

import sys
from datetime import datetime

# input must come from STDIN (standard input)
for line in sys.stdin:

    columns = line.split(',')
    date = columns[1].split('T',1)[0]
    url = columns[2]
    ttfb = str(columns[8]).rstrip()  #floats leave a newline and must be removed

    # output key and value to console, must be tab delimited, must be strings
    print( '%s\t%s' % (date, url+"::"+ttfb)  )
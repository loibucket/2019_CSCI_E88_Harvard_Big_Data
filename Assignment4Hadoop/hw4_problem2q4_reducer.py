#!/usr/bin/env python
'''
reducer
Query4
get count of unique URLs by country by hour for a  specified time range [t1-t2]
where t1 = 09/13/2019 5PM UTC and
t2 = 09/14/2019 9AM UTC
Both range ends are inclusive
'''

#Loi Cheng
#9/xx/2019

import sys

# input must come from STDIN (standard input) and is the output from the mapper
# print( '%s\t%s' % (dateHour+'::'+country+'::'+url, 1)  )
# the keys comes presorted, so we can use the wordcount logic

# read first line
oldKey = sys.stdin.readline().split('\t', 1)[0]
count = 1

newKey = None

for line in sys.stdin:

    # parse the input we got from mapper
    newKey = line.split('\t', 1)[0]

    # do something only if new and old key are different
    if newKey != oldKey:

        # check if dateHourCountry has changed
        newKeyDateHourCountry = ",".join(newKey.split("::", 2)[:2])
        oldKeyDateHourCountry = ",".join(oldKey.split("::", 2)[:2])

        # it's a new DateHourCountry
        if newKeyDateHourCountry != oldKeyDateHourCountry:
            # write result to STDOUT
            print( '%s\t%s' % (oldKeyDateHourCountry, count) )
            # set up variables based on newKey
            oldKey = newKey
            count = 1

        # it's a new unique url
        else:
            count += 1
            oldKey = newKey

# output the last pair
if newKey == oldKey:
    print( '%s\t%s' % (  "".join(oldKey.split("::", 2)[:2])  , count) )
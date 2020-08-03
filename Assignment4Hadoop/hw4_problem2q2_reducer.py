#!/usr/bin/env python
'''
reducer
Query2: get count of unique visitors per URL per hour
'''

#Loi Cheng
#9/xx/2019

import sys

# input must come from STDIN (standard input) and is the output from the mapper
# print( '%s\t%s' % (dateHour+'::'+url+'::'+user, 1)  )
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

        # check if dateHourUrl has changed
        newKeyDateHourUrl = ":".join(newKey.split("::", 2)[:2])
        oldKeyDateHourUrl = ":".join(oldKey.split("::", 2)[:2])

        # it's a new dateHourUrl
        if newKeyDateHourUrl != oldKeyDateHourUrl:
            # write result to STDOUT
            print( '%s\t%s' % (oldKeyDateHourUrl, count) )
            # set up variables based on newKey
            oldKey = newKey
            count = 1

        # it's a new unqiue user
        else:
            count += 1
            oldKey = newKey

# output the last dateHour urlCount pair
if newKey == oldKey:
    print( '%s\t%s' % (  ":".join(oldKey.split("::", 2)[:2])  , count) )
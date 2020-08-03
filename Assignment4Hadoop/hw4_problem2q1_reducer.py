#!/usr/bin/env python
'''
reducer
Query1: get count of unique URLs per hour

reference
https://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/
'''

#Loi Cheng
#9/xx/2019

import sys

# input must come from STDIN (standard input)
# expected input is this: dateHour::url <tab> 1
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

        # check if dateHour has changed
        newKeyDateHour = newKey.split('::')[0]
        oldKeyDateHour = oldKey.split('::')[0]

        # it's a new dateHour
        if newKeyDateHour != oldKeyDateHour:
            # write result to STDOUT, in form of dateHour <tab> uniqueUrlCount
            print( '%s\t%s' % (oldKeyDateHour, count) )
            # set up variables based on newKey
            oldKey = newKey
            count = 1
        
        # it's a new unqiue url
        else:
            count += 1
            oldKey = newKey


# output the last dateHour urlCount pair
if newKey == oldKey:
    print( '%s\t%s' % (oldKey.split('::')[0], count) )
#!/usr/bin/env python
'''
reducer
Query3: get count of unique (by UUID) events/clicks per URL per hour (one unique click == one log line in our test log files, since we assume they all have unique UUIDs)
'''

#Loi Cheng
#9/xx/2019

import sys

# input must come from STDIN (standard input) and is the output from the mapper
# print( '%s\t%s' % (dateHour+'::'+url, 1)  )
# the keys comes presorted, so we can use the wordcount logic

# read first line
oldKey = sys.stdin.readline().split('\t', 1)[0]
count = 1

newKey = None

for line in sys.stdin:

    # parse the input we got from mapper
    newKey = line.split('\t', 1)[0]

    # compare newKey and oldKey and process accordingly
    if newKey == oldKey:
        count += 1
    else:
        print( '%s\t%s' % (  oldKey  , count) )
        oldKey = newKey
        count = 1

# output the last dateHour urlCount pair
if newKey == oldKey:
    print( '%s\t%s' % (  oldKey  , count) )
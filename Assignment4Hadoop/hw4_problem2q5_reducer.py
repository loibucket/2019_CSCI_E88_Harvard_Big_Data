#!/usr/bin/env python
'''
reducer
Query5
get top 5 URLs by avg daily TTFB, per each day in the input data set; consider "top" URLs to be those that have the smallest avg TTFB

'''

#Loi Cheng
#9/xx/2019

import sys

def getTopFiveTtfb(date, urlDict):
    # calculate average
    avgTtfbDict = {}

    for url in urlDict:
        avgTtfbDict[url] = urlDict[url][0] / float( urlDict[url][1] )

    for key, value in sorted(avgTtfbDict.items(), key=lambda item: item[1])[:5]:
        print("%s: %s" % (date+'::'+key, value))

# input must come from STDIN (standard input) and is the output from the mapper
# print( '%s\t%s' % (date, url+"::"+ttfb)  )

# process first line
firstLine = sys.stdin.readline().rstrip().split('\t', 1)

urlTtfb = firstLine[1].split('::')
url = urlTtfb[0]
ttfb = float( urlTtfb[1] )

#make dict in format >> { url : [ttfb, ttbcount] }
urlDict = { url : [ttfb, 1] } 

oldDate = firstLine[0]
newDate = None

for line in sys.stdin:

    # parse the input we got from mapper
    line = line.rstrip().split('\t', 1)

    newDate = line[0]
    urlTtfb = line[1].split('::')
    url = urlTtfb[0]
    ttfb = float( urlTtfb[1] )

    # still on same date
    if newDate == oldDate:

        # if newUrl already exists
        if url in urlDict.keys():
            storedTtfb = urlDict[url][0]
            storedCount = urlDict[url][1]
            urlDict[url] = [ storedTtfb + ttfb , storedCount + 1]

        # if newUrl is brand new
        else:
            urlDict[url] = [ttfb, 1]
    
    # received new date
    else:

        #write out results from oldDate
        getTopFiveTtfb(oldDate, urlDict)

        #reset everything
        urlDict = {} 
        urlDict[url] = [ttfb, 1]
        oldDate = newDate

#write out last date
if newDate == oldDate:
    getTopFiveTtfb(oldDate, urlDict)
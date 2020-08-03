#!/usr/bin/env python

filePathsList = ['part-00000','part-00001','part-00002']
outputsDict = {}

for filePath in filePathsList:
    fp = open(filePath, 'r')
    for line in fp:
        line = line.split('\t')
        key = line[0]
        value = int( line[1] )
        
        if key in outputsDict.keys():
            outputsDict[ line[0] ] += value 
        else:
            outputsDict[ line[0] ] = value 

for key in sorted(outputsDict.keys()):
    print(  key+'\t'+str(outputsDict[key])  )
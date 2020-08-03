#hw3_problem3.py
#Loi Cheng
#9/19/2019

import argparse
import redis
import pandas as pd

def processData(fileName,redisClient):
    '''
    reduces a set of data to useful lists, which will then be uploaded to a redis server
    '''

    #read file, add the DateTime
    df = pd.read_csv(fileName,header=None)
    df['DateTime'] = pd.to_datetime(df[1])
 
    #create the DateURL column
    df = df.rename(columns={2:"url",8:"TTFB"})
    df['Date'] = df['DateTime'].dt.date
    df['DateURL'] = df['Date'].astype('str') + "::" + df['url'].astype('str')

    # Query5
    # reduce to list DateURL and Sum TTFB and Count TTFB

    dfDateUrlTTFB = df[['DateURL','TTFB']]

    dfDateUrlTTFBSum = dfDateUrlTTFB.groupby('DateURL').sum().reset_index() 
    dfDateUrlTTFBSum = dfDateUrlTTFBSum.rename(columns={'TTFB':'TTFBSum'})

    dfDateUrlTTFBCount = dfDateUrlTTFB.groupby('DateURL').count().reset_index() 
    dfDateUrlTTFBCount = dfDateUrlTTFBCount.rename(columns={'TTFB':'TTFBCount'})

    dfQueryFive = dfDateUrlTTFBSum.set_index('DateURL').join(dfDateUrlTTFBCount.set_index('DateURL')).reset_index() 
    dateUrlTTFBList = dfQueryFive.values.tolist()
    
    # update redis with the new data
    # update the TTFBSum and TTFBCount for each DateURL
    # compute the new average TTFB and update it in the sorted set
    for [DateURL, TTFBSum, TTFBCount] in dateUrlTTFBList:
        key = DateURL + ':TTFBSum:TTFBCount'
        redisClient.hincrbyfloat(  key , 'TTFBSum' , TTFBSum  )
        redisClient.hincrby(  key , 'TTFBCount' , TTFBCount  )

        averageTTFB = float( redisClient.hmget(key , 'TTFBSum')[0] ) / float( redisClient.hmget(key , 'TTFBCount')[0] )

        DateURLSplit = DateURL.split('::')
        Date = DateURLSplit[0]
        URL = DateURLSplit[1]
        redisClient.zadd( Date +":Average_TTFB" , {URL:averageTTFB} )

    print('file processed: ' + fileName)

    return

def main():

    # get input arguments
    parser = argparse.ArgumentParser(description='reads in one input file, processes it line by line, and updates some counters/data structures in Redis')
    parser.add_argument('file', type=str, help='name of file to process')
    args = parser.parse_args()

    redisClient = redis.StrictRedis(host='redis_server', port=6379, password='1234')

    # process data from file
    processData(args.file, redisClient)

if __name__ == '__main__': 
    main()
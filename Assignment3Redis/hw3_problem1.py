#hw3_problem1.py
#Loi Cheng
#9/19/2019

import argparse
import redis
import pandas as pd

def processData(fileName, redisClient):
    '''
    reduces a set of data to useful lists, which will then be uploaded to a redis server
    '''

    df = pd.read_csv(fileName,header=None)
    df['DateTime'] = pd.to_datetime(df[1])
    df['DateHour'] = df['DateTime'].dt.date.astype('str') + ':' + df['DateTime'].dt.strftime('%H').astype('str')
    df['DateHour_URL'] = df['DateHour'].astype('str') + ':' + df[2].astype('str')
    df = df.rename(columns={0:'UUID', 2: 'url', 3: 'user'})

    # Query1
    # reduce to list with hour and unique url for that hour
    dfQueryOne = df[['DateHour','url']].drop_duplicates()
    hourUrlList = dfQueryOne.values.tolist()

    # update redis with the new data, with 'DateHour' as the key, and the corresponding set of unique urls as the value
    # get the number of unique urls after the update and store it in another key
    for [DateHour, url] in hourUrlList:
        key = DateHour + ':all_unique_urls'
        redisClient.sadd(  key , url  )
        uniqueUrlCount = redisClient.scard(  key  )
        redisClient.set(  DateHour + ':unique_url_count', uniqueUrlCount  )

    # Query2
    # reduce data to list with hour unique URL, and unique visitor list 

    dfQueryTwo = df[['DateHour_URL','user']].drop_duplicates()
    hourUrlUserList = dfQueryTwo.values.tolist()

    # update redis with the new data, using similar structure and process as Query 1
    for [DateHour_URL, user] in hourUrlUserList:
        key = DateHour_URL + ':all_unique_users'
        redisClient.sadd(  key , user  )
        uniqueUserCount = redisClient.scard(  key  )
        redisClient.set(  DateHour_URL + ':unique_user_count' , uniqueUserCount  )

    # Query3
    # Reduce data to table with count of unique (by UUID) events/clicks per URL per hour per day
    # <date:hour:url>,  Total Event_count

    dfQueryThree = df[['DateHour_URL','UUID']].groupby('DateHour_URL').count().reset_index() 
    hourUrlUUIDList = dfQueryThree.values.tolist()

    # update redis with the new data, using similar structure and process as Query 1
    # do not update if the file has already been processed into redis
    if redisClient.sismember(  'Query3:Files' , fileName  ) != 1 :
        for [DateHour_URL, UUIDcount] in hourUrlUUIDList:
            redisClient.incrby(  DateHour_URL + ':event_count' , UUIDcount  )
            redisClient.sadd(  'Query3:Files' , fileName  )

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

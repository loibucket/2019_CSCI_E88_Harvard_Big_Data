#hw3_problem2.py
#Loi Cheng
#9/19/2019

import argparse
import redis
import pandas as pd

def processData(fileName,t1,t2,redisClient):
    '''
    reduces a set of data to useful lists, which will then be uploaded to a redis server
    '''

    #read file, add the DateTime
    df = pd.read_csv(fileName,header=None)
    df['DateTime'] = pd.to_datetime(df[1])
 
    #trim file by t1 t2
    df = df.set_index(df['DateTime'])
    df = df.loc[t1:t2]

    #create the DateHour_Country column
    df = df.rename(columns={2: 'url',4:'country'})
    df['DateHour'] = df['DateTime'].dt.date.astype('str') + ':' + df['DateTime'].dt.strftime('%H').astype('str')
    df['DateHour_Country'] = df['DateHour'].astype('str') + ',' + df['country'].astype('str')

    # Query4
    # reduce to list with hour,country and unique url
    dfQueryFour = df[['DateHour_Country','url']].drop_duplicates()
    hourCountryUrlList = dfQueryFour.values.tolist()
    
    # update redis with the new data, with 'DateHour_Country' as the key, and the corresponding set of unique urls as the value
    # get the number of unique urls after the update and store it in another key
    for [DateHour_Country, url] in hourCountryUrlList:
        key = DateHour_Country + ':all_unique_urls'
        redisClient.sadd(  key , url  )
        uniqueUrlCount = redisClient.scard(  key  )
        redisClient.set(  DateHour_Country + ':unique_url_count', uniqueUrlCount  )

    print('file processed: ' + fileName)

    return

def main():

    # get input arguments
    parser = argparse.ArgumentParser(description='reads in one input file, processes it line by line, and updates some counters/data structures in Redis')
    parser.add_argument('file', type=str, help='name of file to process')
    args = parser.parse_args()

    redisClient = redis.StrictRedis(host='redis_server', port=6379, password='1234')

    #specify time range
    t1 = '2019-09-13 05:00:00'
    t2 = '2019-09-14 09:00:00'

    # process data from file
    processData(args.file, t1, t2, redisClient)

if __name__ == '__main__': 
    main()

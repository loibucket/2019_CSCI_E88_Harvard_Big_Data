from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra import util
import pandas as pd
import numpy as np
import time

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

# Initialize the connection and session with Cassandra on localhost
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('hw10')

def handle_success_insert(data,event,insertions):
    print(event)
    insertions[0] += 1
    print('async insertion ok')

def handle_error_insert(error,event):
    print(error)
    print(event)
    print('async insertion error')

def insert_cassandra(num_insertions,timeout):
    """
    Table Reference
    CREATE TABLE IF NOT EXISTS hw10.hw10_p4 (
    id UUID,
    time timestamp,
    url text,
    userId text,
    country text,
    ua_browser text,
    ua_os text,
    response_status int,
    TTFB float,
    hour timestamp,
    PRIMARY KEY ((url, country, hour), time, id)
    );
    """

    #insertions counter, used for pass by reference later
    insertions = [0]

    #use input file data from earlier homeworks
    #data read and pre-formatting
    df = pd.read_csv('file-input1.csv',header=None)
    df = df[~df[4].isna()]
    df[9] = df[1].astype(str).apply(lambda x: x[:13]+':00:00Z')
    df[0] = df[0].astype(str).apply(lambda s: s[:8] + '-' + s[8:12] + '-' + s[12:16] + '-' + s[16:20] + '-' + s[20:])
    #reduce number of countries and urls for homework purposes
    new_url_list = ['http://example.com/?url=091','http://example.com/?url=095','http://example.com/?url=099']
    new_country_list = ['ER','SJ','MA','GD','ZW']
    u = np.random.choice(new_url_list, size=len(df))
    c = np.random.choice(new_country_list, size=len(df))
    df[2] = u
    df[4] = c

    futures = []

    #run insertions
    for i in range(num_insertions):

        #format each entry for insertion
        #proper format example
        #event_string = "8e66dea6-2a91-4ba6-9e48-c534c705574e, '2019-09-14 03:56:26Z', 'http://example.com/?url=078', 'user-079', 'SN', 'Firefox', 'Mac', 201, 0.2186, '2019-09-14 03:00:00Z'"
        event_string = str(list(df.iloc[i]))[1:-1]
        event_string = event_string.replace("'","",2)
        #print("event", event_string  )
    
        query = SimpleStatement(
            """
            INSERT INTO hw10.hw10_p2
            (id, time, url, userId, country, ua_browser, ua_os, response_status, TTFB, hour)
            VALUES ( %s )
            """ 
            %event_string,
            consistency_level=ConsistencyLevel.QUORUM
        )

        #async append
        futures.append (
            session.execute_async(query)
        )

        #setup callbacks
        futures[-1].add_callbacks(callback = handle_success_insert, errback = handle_error_insert, callback_args=(event_string,insertions,),errback_args=(event_string,))

    wait_timeout = 0
    while insertions[0] < num_insertions:
        print ('insertions completed', insertions[0])
        time.sleep(1)
        wait_timeout += 1
        if wait_timeout > timeout:
            print('timeout, some insertions were not made')
            break
        
def read_cassandra():

    query = SimpleStatement(
        """
        SELECT url, country, count(*), AVG(TTFB) FROM hw10.hw10_p2
        WHERE hour = '2019-09-12 05:00:00Z' and 
        country = 'ZW' and 
        url = 'http://example.com/?url=095' and 
        time >= '2019-09-12 05:00:00Z' and time <= '2019-09-12 05:40:00Z'
        """,
        consistency_level=ConsistencyLevel.QUORUM
    )

    row = session.execute(query)
    for items in row:
        print(items)

    query = SimpleStatement(
        """
        SELECT url, country, count(*), AVG(TTFB) FROM hw10.hw10_p2
        WHERE hour = '2019-09-12 01:00:00Z' and 
        country = 'SJ' and 
        url = 'http://example.com/?url=099' and 
        time >= '2019-09-12 01:00:00Z' and time <= '2019-09-12 01:40:00Z';
        """,
        consistency_level=ConsistencyLevel.QUORUM
    )

    row = session.execute(query)
    for items in row:
        print(items)

def main():

    #clean the table for homework purposes only
    session.execute(
    """
    TRUNCATE hw10.hw10_p4
    """
    )

    #async insert, but will either wait until all inserts are done, or timeout
    num_insertions = 10
    timeout = 5
    insert_cassandra(num_insertions,timeout)

    #read
    read_cassandra()

    print("Job Completed")

if __name__ == "__main__":
    main()

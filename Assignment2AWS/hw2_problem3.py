#Loi Cheng
#9/13/2019

import multiprocessing 
import pandas as pd

def processData(fileName, hourUrlList, hourUrlUserList, hourUrlUUIDList):
    """
    reduces a set of data to useful lists that will be combined with similar lists generated from other threads
    """

    df = pd.read_csv(fileName,header=None)
    df["DateTime"] = pd.to_datetime(df[1])
    df["DateHour"] = df["DateTime"].dt.date.astype("str") + ":" + df["DateTime"].dt.strftime("%H").astype("str")
    df['DateHour_URL'] = df['DateHour'].astype('str') + ":" + df[2].astype('str')
    df = df.rename(columns={0:"UUID", 2: "url", 3: "user"})

    # Query1
    # reduce to list with hour and unique url
    dfQueryOne = df[["DateHour","url"]].drop_duplicates()
    hourUrlList += dfQueryOne.values.tolist()
    
    # Query2
    # reduce data to list with hour unique URL, and unique visitor list 

    dfQueryTwo = df[['DateHour_URL','user']].drop_duplicates()
    hourUrlUserList += dfQueryTwo.values.tolist()

    # Query3
    # Reduce data to table with count of unique (by UUID) events/clicks per URL per hour per day
    # <date:hour:url>,  Total Event_count

    dfQueryThree = df[['DateHour_URL','UUID']].groupby('DateHour_URL').count().reset_index() 
    hourUrlUUIDList += dfQueryThree.values.tolist()

    return

if __name__ == "__main__": 

    with multiprocessing.Manager() as manager: 
        # creating lists in server process memory 
        hourUrlList = manager.list() 
        hourUrlUserList = manager.list() 
        hourUrlUUIDList = manager.list() 
  
        # creating new processes 
        p1 = multiprocessing.Process(target=processData, args=("input_files/file-input1.csv", hourUrlList, hourUrlUserList, hourUrlUUIDList)) 
        p2 = multiprocessing.Process(target=processData, args=("input_files/file-input2.csv", hourUrlList, hourUrlUserList, hourUrlUUIDList)) 
        p3 = multiprocessing.Process(target=processData, args=("input_files/file-input3.csv", hourUrlList, hourUrlUserList, hourUrlUUIDList)) 
        p4 = multiprocessing.Process(target=processData, args=("input_files/file-input4.csv", hourUrlList, hourUrlUserList, hourUrlUUIDList)) 
  
        # running process
        p1.start() 
        p2.start() 
        p3.start() 
        p4.start() 

        # wait to finish
        p1.join() 
        p2.join() 
        p3.join() 
        p4.join() 
  
        # process joined data for Query1
        df = pd.DataFrame.from_records(hourUrlList)
        df = df.drop_duplicates().groupby(0).count().reset_index() 
        df = df.rename(columns={0: "<date_hour>", 1: "<url_count>"})
        df.to_csv("date_hour--url_count.csv", index=None)

        # output joined data for Query2
        df = pd.DataFrame.from_records(hourUrlUserList)
        df = df.drop_duplicates().groupby(0).count().reset_index() 
        df = df.rename(columns={0: "<date:hour:url>", 1: "unique_user_count"})
        df.to_csv("date_hour_url--unique_user_count.csv", index=None)

        # output joined data for Query3
        df = pd.DataFrame.from_records(hourUrlUUIDList)
        df = df.groupby(0).sum().reset_index() 
        df = df.rename(columns={0: "<date:hour:url>", 1: "Total Event_count"})
        df.to_csv("date_hour_url--Total_Event_count.csv", index=None)
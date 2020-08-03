#Loi Cheng
#9/13/2019

import multiprocessing 
import pandas as pd

def processData(fileName, hourCountryUrlList, t1, t2):
    '''
    reduces a set of data to useful lists that will be combined with similar lists generated from other threads
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
    hourCountryUrlList += dfQueryFour.values.tolist()
    
    return

if __name__ == '__main__': 

    #specify time range
    t1 = '2019-09-13 17:00:00'
    t2 = '2019-09-14 09:00:00'

    with multiprocessing.Manager() as manager: 
        # creating lists in server process memory 
        hourCountryUrlList = manager.list() 
  
        # creating new processes 
        p1 = multiprocessing.Process(target=processData, args=('input_files/file-input1.csv', hourCountryUrlList, t1, t2) )
        p2 = multiprocessing.Process(target=processData, args=('input_files/file-input2.csv', hourCountryUrlList, t1, t2) )
        p3 = multiprocessing.Process(target=processData, args=('input_files/file-input3.csv', hourCountryUrlList, t1, t2) )
        p4 = multiprocessing.Process(target=processData, args=('input_files/file-input4.csv', hourCountryUrlList, t1, t2) )
  
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
  
        # process joined data for Query4
        df = pd.DataFrame.from_records(hourCountryUrlList)
        df = df.drop_duplicates().groupby(0).count().reset_index() 
        df = df.rename(columns={0: '<date_hour_country>', 1: '<url_count>'})
        df.to_csv('date_hour_country--url_count.csv', index=None)
#Loi Cheng
#9/13/2019

import multiprocessing 
import pandas as pd

def processData(fileName, dateUrlTTFBList):
    '''
    reduces a set of data to useful lists that will be combined with similar lists generated from other threads
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
    dateUrlTTFBList += dfQueryFive.values.tolist()

    return

if __name__ == '__main__': 

    with multiprocessing.Manager() as manager: 
        # creating lists in server process memory 
        dateUrlTTFBList = manager.list() 
  
        # creating new processes 
        p1 = multiprocessing.Process(target=processData, args=('input_files/file-input1.csv', dateUrlTTFBList)  )
        p2 = multiprocessing.Process(target=processData, args=('input_files/file-input2.csv', dateUrlTTFBList)  )
        p3 = multiprocessing.Process(target=processData, args=('input_files/file-input3.csv', dateUrlTTFBList)  )
        p4 = multiprocessing.Process(target=processData, args=('input_files/file-input4.csv', dateUrlTTFBList)  )
  
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
        df = pd.DataFrame.from_records(dateUrlTTFBList)
        df = df.groupby(0).sum().reset_index() 

        df3 = df[0].str.split("::", n = 1, expand = True)
        df3['averageTTFB']=df[1]/df[2]
        df3 = df3.rename(columns={0:"date",1:"url"})

        outDF = df3.sort_values(['date','averageTTFB']).groupby('date').head(5)
        outDF.to_csv('date--fiveUrl--LowestAvgTTFB.csv', index=None)
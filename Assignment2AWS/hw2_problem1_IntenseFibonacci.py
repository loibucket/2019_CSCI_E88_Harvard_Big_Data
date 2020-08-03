#Loi Cheng
#9/12/2019

import time
import multiprocessing 
import os
import psutil
import argparse

def fibonacciBillion():
    '''
    Sleeps for 1 sec
    Prints its name/ID to the console
    Performs CPU-intensive work, calculate a Fibonacci sequence for around one billion iterations
    Does the above three things forever (you can use an endless while loop)
    '''

    while(True):

        time.sleep(1)
        processID = os.getpid()
        process = psutil.Process(processID)
        print(  "Process_Name: " + process.name() + " Process_ID: " + str(processID)  )

        lastNum = 1
        nextNum = 1

        for i in range (10000000000):
            newSum = lastNum + nextNum
            lastNum = nextNum
            nextNum = newSum

    return

if __name__ == "__main__": 

    # get input arguments
    parser = argparse.ArgumentParser(description='IntenseFibonacci')
    parser.add_argument('numThreads', type=int, help='number of threads')
    args = parser.parse_args()

    # run specified number of numThreads
    for i in range(args.numThreads):
        p = multiprocessing.Process(target=fibonacciBillion)
        p.start()
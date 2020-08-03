#Loi Cheng
#9/13/2019

import time
import multiprocessing 
import os
import psutil
import argparse
import random

def bigIOtask():
    '''
    Continuously create, write 10000 random numbers to, close and delete a file
    '''

    processID = os.getpid()
    process = psutil.Process(processID)
    print(  "Process_Name: " + process.name() + " Process_ID: " + str(processID)  )

    while(True):

        fileName = "big_IO_file_" + process.name() + "_" + str(processID) + ".txt"

        # open a (new) file to write
        outF = open(fileName, "w")

        for i in range (10000):
            # write line to output file
            outF.write( str(random.randint(1,10000)) )
            outF.write("\n")

        outF.close()

        os.remove(fileName) 

    return

if __name__ == "__main__": 

    # get input arguments
    parser = argparse.ArgumentParser(description='bigIOtask')
    parser.add_argument('numThreads', type=int, help='number of threads')
    args = parser.parse_args()

    # run specified number of numThreads
    for i in range(args.numThreads):
        p = multiprocessing.Process(target=bigIOtask)
        p.start()
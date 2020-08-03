import random
import threading
import argparse

def makeFile(fileName, numLines):
    '''
    generate a file with the specified number of lines:
    each line should have 3 random numbers in the range [0-10]; the lines would look like:
    1 7 3
    2 1 3
    '''
    # open a (new) file to write
    outF = open(fileName, "w")

    for line in range (numLines):
        # write line to output file
        outF.write( str(random.randint(1,10)) + " " + str(random.randint(1,10)) + " " + str(random.randint(1,10))  )
        outF.write("\n")

    outF.close()

def makeManyFiles(numFiles, numLines, filePrefix):
    '''
    generate specified number of files with the specified number of lines:
    each line should have 3 random numbers in the range [0-10]; the lines would look like:
    1 7 3
    2 1 3
    
    File names are in the format: <yourFirstName>_<yourLastName>_threadNumber.txt
    For example, if 'numFiles' = 3, the following files should be generated:
    marina_popova_0.txt
    marina_popova_1.txt
    marina_popova_2.txt
    '''   
    for fileCount in range (numFiles):
        
        fileName = filePrefix + str(fileCount) + ".txt"
        #run thread
        threading.Thread(target=makeFile, args=(fileName,numLines)).start()

def main():

    # get input arguments
    parser = argparse.ArgumentParser(description='hw1_problem1')
    parser.add_argument('numFiles', type=int, help='number of files')
    parser.add_argument('numLines', type=int, help='number of lines')
    args = parser.parse_args()

    print(args)

    # make many files
    makeManyFiles(args.numFiles, args.numLines, "loi_cheng_")

if __name__ == "__main__": 
    main()

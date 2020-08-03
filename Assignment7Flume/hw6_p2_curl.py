# Write a program that will issue the curl command 
# at a rate of x times per second where x is a configurable parameter. The program should run in an endless loop. 

import sys
import os
import time
from subprocess import call

rate = int(sys.argv[1])

# Time to sleep between requests
wait_time = 1.0 / rate

# Write the output to the /dev/null as we are not interested in the content
# Count number of requests
count = 0
with open(os.devnull,'w') as DEVNULL:
    while True:
        call(['curl','-s','http://localhost:80'], stdout=DEVNULL)
        count += 1
        if count % rate == 0:
            print(count)
        time.sleep(wait_time)
import argparse

import redis

prog = "lab3_redis"
desc = "Process data provided by log files and return query results"
parser = argparse.ArgumentParser(prog=prog, description=desc)
parser.add_argument('--logs-directory', '-ld', required=False,
                    help="Directory where the log files are stored.")
parser.add_argument('--file_name', '-file', required=False,
                    help="Name of the  log file are stored")



# Parse out the hour
def extract_hour ( timestamp ):
    return timestamp [:13]


# Calculate the answer for the lab question - find number of events per hour
def calc_lab3(file_name, url_dict):

    with open (file_name,'r') as f:
        lines = f.readlines()

    for l in lines:
        cols = l.strip().split(",")
        url = cols[2]
        hour = extract_hour(cols[1])

# Insert a new key with initial value 1. If that key already exists, then the function returns 0
        if redisClient.hsetnx("hourly_event",hour,1) == 0:
# Atomically increment the value for key by 1 if the key already exists
            redisClient.hincrby("hourly_event",hour)
    return


parsed_args = parser.parse_args()

logs_dir = parsed_args.logs_directory

#print (logs_dir)

if logs_dir is None:
    logs_dir = "input_files/"
elif not logs_dir.endswith("/"):
    logs_dir = logs_dir + "/"


file_name = parsed_args.file_name
#print (file_name)
if file_name is None:
    file_name = "file-564899aad2264f9bbc97dabaa879a017.csv"


file_name = logs_dir + file_name


# Create a redis client

redisClient = redis.StrictRedis(host='localhost',
                                port=6379)

url_info = {}


calc_lab3(file_name, url_info)


#print ("-------------query 1 Completed--------------")

print "Process Completed"


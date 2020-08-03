import argparse
from random import randint
from time import sleep
import multiprocessing.dummy

COLUMN_SEPARATOR = " "
NEW_LINE_CHAR = "\n"

prog = "lab2_mpf"
desc = "Generate specified number of files with with random numbers"
parser = argparse.ArgumentParser(prog=prog, description=desc)
parser.add_argument('--file', '-f', default="output.txt")
parser.add_argument('--line-count', '-l', default=10000, type=int)
parser.add_argument('--column-count', '-c', default=500, type=int)
parser.add_argument('--file-count', '-fc', default=5000, type=int)

parsed_args = parser.parse_args()
file_to_process = parsed_args.file
line_count = parsed_args.line_count
column_count = parsed_args.column_count
file_count = parsed_args.file_count

def get_randomn_line_entries(column_count=3):
    line_val=""
    for column_no in range(column_count):
        if column_no != column_count-1:
            line_val += str(randint(0, 9)) + COLUMN_SEPARATOR
        else:
            line_val += str(randint(0, 9))

    return line_val


def create_file(file_name, line_cnt=10, column_cnt=3):
    while True:
        file_to_process = open(file_name, "w")
        for line_number in range(line_cnt):
            if line_number != line_cnt - 1:
                file_to_process.write(get_randomn_line_entries(column_cnt) + NEW_LINE_CHAR)
            else:
                file_to_process.write(get_randomn_line_entries(column_cnt))
        file_to_process.close()
        print "generated file: ", file_name
        sleep(1)  # sleep for 1 sec

jobs = []
for file_count_id in range(file_count):
    file_name_with_suffix = file_to_process + str(file_count_id)
    t = multiprocessing.Process(target=create_file, args=(file_name_with_suffix, line_count, column_count))
    jobs.append(t)
    t.start()  # new child process is started at this point, it has its own execution flow

for curr_job in jobs:
    curr_job.join()

print "Process Completed"
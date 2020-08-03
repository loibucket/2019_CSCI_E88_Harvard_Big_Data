#!/usr/bin/python3

import sys
# import avro.schema
# from avro.datafile import DataFileReader, DataFileWriter
# from avro.io import DatumReader, DatumWriter
import fileinput
#from cs88_log_parser1 import parse_log_line_w3
from collections import namedtuple
from datetime import datetime

LogLineW3 = namedtuple('LogLineW3', ['timestamp', 'url', 'user_id'])

def parse_log_line_w3(line):
    tokens = line.strip().split("\t")
    if len(tokens) < 3:
        return None
    else:
        try:
            event_datetime = datetime.strptime(tokens[0], '%Y-%m-%dT%H:%M:%S.%fZ')
            return LogLineW3(event_datetime, tokens[1], tokens[2])
        except ValueError as e:
            try:
                event_datetime = datetime.strptime(tokens[0], '%Y-%m-%dT%H:%M:%SZ')
                return LogLineW3(event_datetime, tokens[1], tokens[2])
            except ValueError as e:
                return None

#out_file = open("map_out.txt", "w")

for line in sys.stdin:
#for line in fileinput.input():
    # parse the log line - and get the timestamp only
    # convert to a format we want - only date and hour - since we are only interested in the hour
    parsed_line = parse_log_line_w3(line)
    if parsed_line is not None:
        event_hour = parsed_line.timestamp.strftime("%y-%m-%d %H")
        print ("%s\t%d" % (event_hour, 1))
        #out_file.write("%s\t%d" % (event_hour, 1))

#out_file.close()
print ("Mapper is done")

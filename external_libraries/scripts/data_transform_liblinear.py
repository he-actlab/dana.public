#!/usr/bin/env python3
import csv
import sys
import os
import re
import time

start = time.time()

library = "liblinear"

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

if(len(sys.argv) < 2):
    print (bcolors.FAIL + "ERROR: input file not provided." + bcolors.ENDC)
    exit()

file_in = sys.argv[1]
file_in_name, file_extension = os.path.splitext(file_in)

if(len(sys.argv) > 2):
    file_out = sys.argv[2]
else:
    file_out = file_in_name + "_" + library + "_output"
    print (bcolors.WARNING + "Warning: output file name not specified. Writing to " + file_out + bcolors.ENDC)

file_out = open(file_out,"w")

with open(file_in) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        for key, value in row.items():
            if(key == "y"):
                if(float(value) > 0): file_out.write("+1" + " ")
                elif(float(value) <= 0): file_out.write("-1" + " ")
            elif(key == "features"):
                i = 1
                features = re.split(",|}|{|", value)
                features[:] = [item for item in features if item != '']
                for feature in features:
                    if(float(feature) != 0):
                    	file_out.write(str(i) + ":" + feature + " ")
                    i = i + 1
                file_out.write("\n")

end = time.time()
total_time = (end - start)*10**6
print (total_time)

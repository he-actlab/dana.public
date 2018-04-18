#!/usr/bin/python

import sys
import numpy
import random

def genDatasets(readfile):
    row = 0
    with open(readfile, "r") as r:
        with open("out.txt", "w") as w:
            for line in r:
                row = row + 1
                content = line.strip().split("  ")
                #content = content[-1].strip()
                #print content
                for i in range(0,len(content)):
                    if(float(content[i]) != 0.0):
                        writeword = str(i+1) + "," + str(row) + "," + str(float(content[i])) + "\n"
                        w.write(writeword)
            w.close()
            r.close()

if __name__ == '__main__':
    if (sys.argv[len(sys.argv)-1] == "-h" or sys.argv[len(sys.argv)-1] == "-help"):
        print "USAGE: python reco-data-transform <readfile>"
        exit()
    
    elif (len(sys.argv) != 2):
        print "ERROR!!"
        print "USAGE: python reco-data-transform <readfile>"
        exit()

    random.seed()
    genDatasets(sys.argv[1])

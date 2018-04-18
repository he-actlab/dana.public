#!/usr/bin/python

import sys
import csv
import numpy as np
import random

'''
LRMF data file format
colum, row, rank
'''

def get_maxrow(filename):
    maxrow = -1
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        for line in reader:
            row = int(line[1])
            if row > maxrow:
                maxrow = row
    return maxrow

def get_maxcol(filename):
    maxcol = -1
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        for line in reader:
            col = int(line[0])
            if col > maxcol:
                maxcol = col
    return maxcol

def transform(filename):
    row_size = get_maxrow(filename)
    col_size = get_maxcol(filename)
    arr = np.zeros((row_size, col_size), dtype=np.float)
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        for line in reader:
            col = int(line[0])
            row = int(line[1])
            val = float(line[2])
            arr[row - 1][col - 1] = val
    return arr, row_size, col_size

def writeto(filename, matrix, numrow, numcol):
    with open(filename, 'w') as f:
        f.write(str(numrow) + '\n')
        f.write(str(numcol) + '\n')
        for row in matrix:
            for num in row:
                f.write(str(num) + ' ')
            f.write('\n')

def gen_matrix(row, col):
    matrix = [[random.uniform(0, 1) for _ in range(col)] for __ in range(row)]
    return matrix

def write_param(filename, numcol):
    matrix = gen_matrix(numcol, 10)
    with open(filename, 'w') as f:
        for row in matrix:
            for num in row:
                # f.write('%.2f'%(num) + ' ')
                f.write(str(num) + ' ')
            f.write('\n')

def write_feature(filename, numrow):
    matrix = gen_matrix(numrow, 10)
    with open(filename, 'w') as f:
        for row in matrix:
            for num in row:
                f.write(str(num) + ' ')
            f.write('\n')

if __name__ == '__main__':
    filename = sys.argv[1]
    arr, numrow, numcol = transform(filename)
    #writeto('lrmf-synthetic-transformed.txt', arr, numrow, numcol)
    write_param('lrmf-synthetic-param.txt', numcol)
    write_feature('lrmf-synthetic-feature.txt', numrow)

#! /usr/bin/python
import random
import sys

infile = sys.argv[1]   # input file name
outfile = sys.argv[2]  # output file name

class FASTQ:
    name = ''
    seq = ''
    comment = ''
    quality = ''

i = 0

fastqList = []

file = open(infile, 'r')
for line in file:
    if i % 4 == 0:
        inStr = FASTQ()
        inStr.name = line        
    elif i % 4 == 1:
        inStr.seq = line
    elif i % 4 == 2:
        inStr.comment = line
    else:
        inStr.quality = line
        fastqList.append(inStr)
    i += 1
file.close()

# permute fastqList
fastqListLen = len(fastqList)
for i in range(fastqListLen):
    j = random.randint(i, fastqListLen - 1)
    tmp = fastqList[i]
    fastqList[i] = fastqList[j]
    fastqList[j] = tmp

# dump output permuted FASTQ file
file = open(outfile, 'w')
for fq in fastqList:
    file.write(fq.name)
    file.write(fq.seq)
    file.write(fq.comment)
    file.write(fq.quality)
file.close()


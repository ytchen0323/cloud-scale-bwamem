#! /usr/bin/python
import random
import sys

infile1 = sys.argv[1]   # input file name: pair-end 1
infile2 = sys.argv[2]   # input file name: pair-end 2
outfile1 = sys.argv[3]  # output file name: pair-end 1
outfile2 = sys.argv[4]  # output file name: pair-end 2

# FASTQ definition
class FASTQ:
    name = ''
    seq = ''
    comment = ''
    quality = ''

fastqList1 = []
fastqList2 = []

# read two FASTQ files
i = 0
file = open(infile1, 'r')
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
        fastqList1.append(inStr)
    i += 1
file.close()

i = 0
file = open(infile2, 'r')
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
        fastqList2.append(inStr)
    i += 1
file.close()

# permute fastqList1 and fastqList2 together!
if len(fastqList1) != len(fastqList2):
    print 'Error: different numbers of reads on two FASTQ files'
else:
    fastqListLen = len(fastqList1)
    for i in range(fastqListLen):
        j = random.randint(i, fastqListLen - 1)
        tmp = fastqList1[i]
        fastqList1[i] = fastqList1[j]
        fastqList1[j] = tmp
        tmp = fastqList2[i]
        fastqList2[i] = fastqList2[j]
        fastqList2[j] = tmp

# dump output permuted FASTQ files
file = open(outfile1, 'w')
for fq in fastqList1:
    file.write(fq.name)
    file.write(fq.seq)
    file.write(fq.comment)
    file.write(fq.quality)
file.close()

file = open(outfile2, 'w')
for fq in fastqList2:
    file.write(fq.name)
    file.write(fq.seq)
    file.write(fq.comment)
    file.write(fq.quality)
file.close()


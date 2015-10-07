#! /usr/bin/python

# *
# * Licensed to the Apache Software Foundation (ASF) under one or more
# * contributor license agreements.  See the NOTICE file distributed with
# * this work for additional information regarding copyright ownership.
# * The ASF licenses this file to You under the Apache License, Version 2.0
# * (the "License"); you may not use this file except in compliance with
# * the License.  You may obtain a copy of the License at
# *
# *    http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# *

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


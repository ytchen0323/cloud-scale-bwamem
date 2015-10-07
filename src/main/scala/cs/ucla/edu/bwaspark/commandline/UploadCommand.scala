/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package cs.ucla.edu.bwaspark.commandline

class UploadFASTQCommand {
  var isPairEnd: Int = 0                // pair-end or single-end data
  var filePartitionNum: Int = 0         // the number of partitions in HDFS of this batch. We suggest to set this number equal to the number of core in the cluster.
  var inputFASTQFilePath1: String = ""  // the first input path of the FASTQ file in the local file system (for both single-end and pair-end)
  var inputFASTQFilePath2: String = ""  // the second input path of the FASTQ file in the local file system (for pair-end)
  var outFileHDFSPath: String = ""      // the root path of the output FASTQ files in HDFS
  var batchedNum: Int = 250000          // (Optional) the number of lines to be read in one group (batch)
}


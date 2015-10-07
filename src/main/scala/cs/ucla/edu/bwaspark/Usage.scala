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


package cs.ucla.edu.bwaspark

object Usage {
  val usage: String = "Usage 1: upload raw FASTQ file(s) to HDFS\n" +
                      "Usage: upload-fastq [-bn INT] isPairEnd filePartitionNum inputFASTQFilePath1 [inputFASTQFilePath2] outFileHDFSPath\n\n" +
                      "Required arguments (in the following order): \n" +
                      "isPairEnd: pair-end (1) or single-end (0) data\n" +
                      "inputFASTQFilePath1: the first input path of the FASTQ file in the local file system (for both single-end and pair-end)\n" +
                      "inputFASTQFilePath2: (optional) the second input path of the FASTQ file in the local file system (for pair-end)\n" +
                      "outFileHDFSPath: the root path of the output FASTQ files in HDFS\n\n" +
                      "Optional arguments: \n" +
                      "-bn (optional): the number of lines to be read in one group (batch)\n\n\n" +
                      "Usage 2: use CS-BWAMEM aligner\n" +
                      "Usage: cs-bwamem [-bfn INT] [-bPSW (0/1)] [-sbatch INT] [-bPSWJNI (0/1)] [-jniPath STRING] [-oType (0/1/2)] [-oPath STRING] [-localRef INT] [-R STRING] [-isSWExtBatched (0/1)] [-bSWExtSize INT] [-FPGAAcc (0/1)] isPairEnd fastaInputPath fastqHDFSInputPath\n\n" +
                      "Required arguments (in the following order): \n" +
                      "isPairEnd: perform pair-end (1) or single-end (0) mapping\n" +
                      "fastaInputPath: the path of (local) BWA index files (bns, pac, and so on)\n" +
                      "fastqHDFSInputPath: the path of the raw read files stored in HDFS\n\n" +
                      "Optional arguments: \n" +
                      "-bfn (optional): the number of raw read folders in a batch to be processed\n" +
                      "-bPSW (optional): whether the pair-end Smith Waterman is performed in a batched way\n" + 
                      "-sbatch (optional): the number of reads to be processed in a subbatch\n" +
                      "-bPSWJNI (optional): whether the native JNI library is called for better performance\n" +
                      "-jniPath (optional): the JNI library path in the local machine\n" +
                      "-oChoice (optional): the output format choice\n" +
                      "                   0: no output (pure computation)\n" +
                      "                   1: SAM file output in the local file system (default)\n" +
                      "                   2: ADAM format output in the distributed file system\n" +
                      "                   3: SAM format output in the distributed file system\n" +
                      "-oPath (optional): the output path; users need to provide correct path in the local or distributed file system\n\n" +
                      "-localRef (optional): specifiy if each node has reference genome locally. If so, our tool can fetch the reference genome from the local node instead of broadcasting it from the driver node. Note that the path of the reference genome should be place at the same path and specified in the \"fastaInputPath parameter\".\n\n" +
                      "-R (should be added for common case): Complete read group header line. Example: @RG\tID:foo\tSM:bar\n\n" +
                      "-isSWExtBatched (optional): whether the SWExtend is executed in a batched way\n" +
                      "                   0: No (default)\n" +
                      "                   1: Yes\n\n" +
                      "-bSWExtSize (optional): the batch size used for SWExtend\n\n" +
                      "-FPGAAccSWExt (optional): whether the FPGA accelerator is used for accelerating SWExtend\n" +
                      "                   0: No (default)\n" +
                      "                   1: Yes\n\n" +
                      "-FPGASWExtThreshold (optional): the threshold of using FPGA accelerator for SWExtend.\n" + 
                      "    If the nubmer of seed in one step is larger than this threshold, FPGA acceleration will be applied. Otherwise, CPU is used for computation.\n\n\n" +
                      "Usage 3: merge the output ADAM folder pieces and save as a new ADAM file in HDFS\n" +
                      "Usage: merge hdfsServerAddress adamHDFSRootInputPath adamHDFSOutputPath\n\n\n" +
                      "Usage 4: sort the output ADAM folder pieces and save as a new ADAM file in HDFS\n" +
                      "Usage: sort hdfsServerAddress adamHDFSRootInputPath adamHDFSOutputPath\n"
}

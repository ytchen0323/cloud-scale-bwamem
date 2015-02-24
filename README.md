cloud-scale-bwamem-0.1.0
===============
Usage 1: upload raw FASTQ file(s) to HDFS
Usage: upload-fastq [-bn INT] isPairEnd filePartitionNum inputFASTQFilePath1 [inputFASTQFilePath2] outFileHDFSPath

Required arguments (in the following order):
isPairEnd: pair-end (1) or single-end (0) data
inputFASTQFilePath1: the first input path of the FASTQ file in the local file system (for both single-end and pair-end)
inputFASTQFilePath2: (optional) the second input path of the FASTQ file in the local file system (for pair-end)
outFileHDFSPath: the root path of the output FASTQ files in HDFS

Optional arguments:
-bn (optional): the number of lines to be read in one batch, which depends on the DRAM you have on the driver node.


Usage 2: use CS-BWAMEM aligner
Usage: cs-bwamem [-bfn INT] [-bPSW (0/1)] [-sbatch INT] [-bPSWJNI (0/1)] [-jniPath STRING] [-oType (0/1/2)] [-oPath STRING] isPairEnd fastaInputPath fastqHDFSInputPath fastqInputFolderNum

Required arguments (in the following order):
isPairEnd: perform pair-end (1) or single-end (0) mapping
fastaInputPath: the path of (local) BWA index files (bns, pac, and so on)
fastqHDFSInputPath: the path of the raw read files stored in HDFS
fastqInputFolderNum: the number of folders generated in the HDFS for the raw reads (output from Usage1). (NOTE: this parameter can be automatically fetched in the next version)

Optional arguments:
-bfn (optional): the number of folders of raw reads to be processed in a batch
-bPSW (optional): whether the pair-end Smith Waterman is performed in a batched way
-sbatch (optional): the number of reads to be processed in a subbatch using JNI library
-bPSWJNI (optional): whether the native JNI library is called for better performance
-jniPath (optional): the JNI library path in the local machine
-oChoice (optional): the output format choice
                   0: no output (pure computation)
                   1: SAM file output in the local file system (default)
                   2: ADAM format output in the distributed file system
-oPath (optional): the output path; users need to provide correct path in the local or distributed file system


Usage 3: merge the output ADAM folder pieces and save as a new ADAM file in HDFS
Usage: merge adamHDFSRootInputPath adamHDFSOutputPath


Usage 4: sort the output ADAM folder pieces and save as a new ADAM file in HDFS
Usage: sort adamHDFSRootInputPath adamHDFSOutputPath

To do list:
(1) Single-end performance tuning
(2) Code merging with Peng version
(3) Kryo serialization integration


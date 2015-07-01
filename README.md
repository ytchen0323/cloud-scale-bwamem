# Cloud-Scale BWAMEM

# Introduction
Cloud-scale BWAMEM (CS-BWAMEM) is an ultrafast and highly scalable aligner built on top of cloud infrastructures, including Spark and Hadoop distributed file system (HDFS). It leverages the abundant computing resources in a public or private cloud to fully exploit the parallelism obtained from the enormous number of reads. With CSBWAMEM, the pair-end whole-genome reads (30x) can be aligned within 80 minutes in a 25-node cluster with 300 cores.

# Build and Install
1. git clone git@github.com:ytchen0323/cloud-scale-bwamem.git
2. cd cloud-scale-bwamem
3. ./compile.pl

# Upload FASTQ file(s) to HDFS
  - commands: upload-fastq [-bn INT] isPairEnd filePartitionNum inputFASTQFilePath1 [inputFASTQFilePath2] outFileHDFSPath
  - Required arguments (in the following order):
    
    (1) isPairEnd: 
      
        1: pair-end

        0: single-end (not fully verified yet)
    
    (2) inputFASTQFilePath1: the first input path of the FASTQ file in the local file system (for both single-end and pair-end)
    
    (3) inputFASTQFilePath2: (optional) the second input path of the FASTQ file in the local file system (for pair-end)
    
    (4) outFileHDFSPath: the root path of the output FASTQ files in HDFS
  - Optional arguments:
    (1) -bn (optional): the number of lines to be read in one batch, which depends on the DRAM you have on the driver node.

# Use CS-BWAMEM aligner
  - commands: cs-bwamem [-bfn INT] [-bPSW (0/1)] [-sbatch INT] [-bPSWJNI (0/1)] [-jniPath STRING] [-oType (0/1/2)] [-oPath STRING] isPairEnd fastaInputPath fastqHDFSInputPath fastqInputFolderNum
  - Required arguments (in the following order):
    
    (1) isPairEnd: 
      
        1: pair-end

        0: single-end (not fully verified yet)
    
    (2) fastaInputPath: the path of BWA index files (bns, pac, and so on). This path is locate at local machine instead of HDFS.
    
    (3) fastqHDFSInputPath: the path of the raw read files stored in HDFS
    
    (4) fastqInputFolderNum: the number of folders generated in the HDFS for the raw reads (output from Usage1). (NOTE: this parameter can be automatically fetched in the next version)
  - Optional arguments:
    
    (1) -bfn (optional): the number of folders of raw reads to be processed in a batch
    
    (2) -bPSW (optional): whether the pair-end Smith Waterman is performed in a batched way
    
    (3) -sbatch (optional): the number of reads to be processed in a subbatch using JNI library
    
    (4) -bPSWJNI (optional): whether the native JNI library is called for better performance
    
    (5) -jniPath (optional): the JNI library path in the local machine
    
    (6) -oChoice (optional): the output format choice

        0: no output (pure computation)
    
        1: SAM file output in the local file system (default)
    
        2: ADAM format output in the distributed file system
    
    (7) -oPath (optional): the output path; users need to provide correct path in the local or distributed file system

# Merge the output ADAM folders 
  - This command merges the output ADAM folders after alignment and then save the output as a new ADAM file in HDFS
  - commands: merge adamHDFSRootInputPath adamHDFSOutputPath

# Sort the output ADAM folders
  - This commands sorts the output ADAM folders after alignment and then save the output as a new ADAM file in HDFS
  - commands: sort adamHDFSRootInputPath adamHDFSOutputPath

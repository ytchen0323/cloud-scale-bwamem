package cs.ucla.edu.bwaspark.commandline

class UploadFASTQCommand {
  var isPairEnd: Int = 0                // pair-end or single-end data
  var filePartitionNum: Int = 0         // the number of partitions in HDFS of this batch. We suggest to set this number equal to the number of core in the cluster.
  var inputFASTQFilePath1: String = ""  // the first input path of the FASTQ file in the local file system (for both single-end and pair-end)
  var inputFASTQFilePath2: String = ""  // the second input path of the FASTQ file in the local file system (for pair-end)
  var outFileHDFSPath: String = ""      // the root path of the output FASTQ files in HDFS
  var batchedNum: Int = 250000          // (Optional) the number of lines to be read in one group (batch)
}


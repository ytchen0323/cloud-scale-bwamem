package cs.ucla.edu.bwaspark.commandline

class BWAMEMCommand {
    var fastaInputPath: String = ""                    // the local BWA index files (bns, pac, and so on)
    var fastqHDFSInputPath: String = ""                // the raw read file stored in HDFS
    var isPairEnd: Boolean = false                     // perform pair-end or single-end mapping
    var fastqInputFolderNum: Int = 0                   // the number of folders generated in the HDFS for the raw reads
    var batchedFolderNum: Int = 4                      // (optional) the number of raw read folders in a batch to be processed
    var isPSWBatched: Boolean = true                   // (optional) whether the pair-end Smith Waterman is performed in a batched way
    var subBatchSize: Int = 10                         // (optional) the number of reads to be processed in a subbatch
    var isPSWJNI: Boolean = true                       // (optional) whether the native JNI library is called for better performance
    var jniLibPath: String = "./target/jniNative.so"   // (optional) the JNI library path in the local machine
    var outputChoice: Int = 1                          // (optional) the output format choice
                                                       //            0: no output (pure computation)
                                                       //            1: SAM file output in the local file system (default)
                                                       //            2: ADAM format output in the distributed file system
    var outputPath: String = "output.sam"              // (optional) the output path; users need to provide correct path in the local or distributed file system
}


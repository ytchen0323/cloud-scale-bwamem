package cs.ucla.edu.bwaspark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.mutable.MutableList

import cs.ucla.edu.bwaspark.datatype._
import cs.ucla.edu.bwaspark.worker1.BWAMemWorker1._
import cs.ucla.edu.bwaspark.worker2.BWAMemWorker2._
import cs.ucla.edu.bwaspark.debug.DebugFlag._
import cs.ucla.edu.bwaspark.fastq._
import cs.ucla.edu.avro.fastq._
import cs.ucla.edu.bwaspark.FastMap.memMain
import cs.ucla.edu.bwaspark.FastMapProfile.memMainProfile
import cs.ucla.edu.bwaspark.commandline._
import cs.ucla.edu.bwaspark.dnaseq._

import org.bdgenomics.adam.rdd.ADAMContext._


object BWAMEMSpark {
  private def bwamemCmdLineParser(argsList: List[String]): BWAMEMCommand = {
    type OptionMap = Map[Symbol, Any]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      list match {
        case Nil => map
        case "-bfn" :: value :: tail =>
                               nextOption(map ++ Map('batchedFolderNum -> value.toInt), tail)
        case "-bPSW" :: value :: tail =>
                               nextOption(map ++ Map('isPSWBatched -> value.toInt), tail)
        case "-sbatch" :: value :: tail =>
                               nextOption(map ++ Map('subBatchSize -> value.toInt), tail)
        case "-bPSWJNI" :: value :: tail =>
                               nextOption(map ++ Map('isPSWJNI -> value.toInt), tail)
        case "-jniPath" :: value :: tail =>
                               nextOption(map ++ Map('jniLibPath -> value), tail)
        case "-oChoice" :: value :: tail =>
                               nextOption(map ++ Map('outputChoice -> value.toInt), tail)
        case "-oPath" :: value :: tail =>
                               nextOption(map ++ Map('outputPath -> value), tail)
        case "-R" :: value :: tail =>
                               nextOption(map ++ Map('headerLine -> value), tail)
        case "-isSWExtBatched" :: value :: tail =>
                               nextOption(map ++ Map('isSWExtBatched -> value.toInt), tail)
        case "-bSWExtSize" :: value :: tail =>
                               nextOption(map ++ Map('swExtBatchSize -> value.toInt), tail)
        case "-FPGAAccSWExt" :: value :: tail =>
                               nextOption(map ++ Map('isFPGAAccSWExtend -> value.toInt), tail)
        case "-FPGASWExtThreshold" :: value :: tail =>
                               nextOption(map ++ Map('FPGASWExtThreshold -> value.toInt), tail)
        case "-jniSWExtendLibPath" :: value :: tail =>
                               nextOption(map ++ Map('jniSWExtendLibPath -> value.toString), tail)
        case isPairEnd ::  inFASTAPath :: inFASTQPath :: fastqInputFolderNum :: Nil =>  
                               nextOption(map ++ 
                                          Map('isPairEnd -> isPairEnd.toInt) ++ 
                                          Map('inFASTAPath -> inFASTAPath) ++
                                          Map('inFASTQPath -> inFASTQPath) ++
                                          Map('fastqInputFolderNum -> fastqInputFolderNum.toInt), list.tail.tail.tail.tail)
        case option :: tail => println("[Error] Unknown option " + option) 
                               exit(1) 
      }
    }
    val options = nextOption(Map(),argsList)
    println(options)

    val bwamemArgs = new BWAMEMCommand
    if(options.get('batchedFolderNum) != None)
      bwamemArgs.batchedFolderNum = options('batchedFolderNum).toString.toInt
    if(options.get('isPSWBatched) != None) {
      val isPSWBatched = options('isPSWBatched).toString.toInt
      if(isPSWBatched == 1)
        bwamemArgs.isPSWBatched = true
      else if(isPSWBatched == 0)
        bwamemArgs.isPSWBatched = false
      else {
        println("[Error] Undefined -bPSW argument" + isPSWBatched)
        exit(1)
      }
    }
    if(options.get('subBatchSize) != None)
      bwamemArgs.subBatchSize = options('subBatchSize).toString.toInt
    if(options.get('isPSWJNI) != None) {
      val isPSWJNI = options('isPSWJNI).toString.toInt
      if(isPSWJNI == 1)
        bwamemArgs.isPSWJNI = true
      else if(isPSWJNI == 0)
        bwamemArgs.isPSWJNI = false
      else {
        println("[Error] Undefined -bPSWJNI argument" + isPSWJNI)
        exit(1)
      }
    }
    if(options.get('jniLibPath) != None)
      bwamemArgs.jniLibPath = options('jniLibPath).toString
    if(options.get('outputChoice) != None) {
      val outputChoice = options('outputChoice).toString.toInt
      if(outputChoice > 3) {
        println("[Error] Undefined -oChoice argument" + outputChoice)
        exit(1)
      }
      bwamemArgs.outputChoice = outputChoice
    }
    if(options.get('outputPath) != None)
      bwamemArgs.outputPath = options('outputPath).toString
    if(options.get('headerLine) != None)
      bwamemArgs.headerLine = options('headerLine).toString
    if(options.get('isSWExtBatched) != None) {
      val isSWExtBatched = options('isSWExtBatched).toString.toInt
      if(isSWExtBatched == 0)
        bwamemArgs.isSWExtBatched = false
      else if(isSWExtBatched == 1)
        bwamemArgs.isSWExtBatched = true
      else {
        println("[Error] Undefined -isSWExtBatched argument" + isSWExtBatched)
        exit(1)
      }
    }
    if(options.get('swExtBatchSize) != None) 
      bwamemArgs.swExtBatchSize = options('swExtBatchSize).toString.toInt
    if(options.get('isFPGAAccSWExtend) != None) {
      val isFPGAAccSWExtend = options('isFPGAAccSWExtend).toString.toInt
      if(isFPGAAccSWExtend == 0)
        bwamemArgs.isFPGAAccSWExtend = false
      else if(isFPGAAccSWExtend == 1)
        bwamemArgs.isFPGAAccSWExtend = true
      else {
        println("[Error] Undefined -FPGAAccSWExt argument" + isFPGAAccSWExtend)
        exit(1)
      }
    }
    if(options.get('FPGASWExtThreshold) != None)
      bwamemArgs.fpgaSWExtThreshold = options('FPGASWExtThreshold).toString.toInt

    val isPairEnd = options('isPairEnd).toString.toInt
    if(isPairEnd == 1)
      bwamemArgs.isPairEnd = true
    else if(isPairEnd == 0) 
      bwamemArgs.isPairEnd = false
    else {
      println("[Error] Undefined isPairEnd argument" + isPairEnd)
      exit(1)
    }
    if(options.get('jniSWExtendLibPath) != None)
      bwamemArgs.jniSWExtendLibPath = options('jniSWExtendLibPath).toString
    bwamemArgs.fastaInputPath = options('inFASTAPath).toString
    bwamemArgs.fastqHDFSInputPath = options('inFASTQPath).toString
    bwamemArgs.fastqInputFolderNum = options('fastqInputFolderNum).toString.toInt  

    println("CS- BWAMEM command line arguments: " + bwamemArgs.isPairEnd + " " + bwamemArgs.fastaInputPath + " " + bwamemArgs.fastqHDFSInputPath + " " + bwamemArgs.fastqInputFolderNum + " " + 
            bwamemArgs.batchedFolderNum + " " + bwamemArgs.isPSWBatched + " " + bwamemArgs.subBatchSize + " " + bwamemArgs.isPSWJNI + " " + bwamemArgs.jniLibPath + " " + bwamemArgs.outputChoice + " " + bwamemArgs.outputPath)

    bwamemArgs
  }


  private def uploadFASTQCmdLineParser(argsList: List[String]): UploadFASTQCommand = {
    type OptionMap = Map[Symbol, Any]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      list match {
        case Nil => map
        case "-bn" :: value :: tail =>
                               nextOption(map ++ Map('batchedNum -> value.toInt), tail)
        // upload two FASTQ files for pair-end mapping
        case isPairEnd :: filePartNum :: inFilePath1 :: inFilePath2 :: outFilePath :: Nil =>  
                               nextOption(map ++ 
                                          Map('isPairEnd -> isPairEnd.toInt) ++ 
                                          Map('filePartNum -> filePartNum.toInt) ++
                                          Map('inFilePath1 -> inFilePath1) ++
                                          Map('inFilePath2 -> inFilePath2) ++
                                          Map('outFilePath -> outFilePath), list.tail.tail.tail.tail.tail)
        // upload one FASTQ file for single-end mapping
        case isPairEnd :: filePartNum :: inFilePath :: outFilePath :: Nil =>  
                               nextOption(map ++ 
                                          Map('isPairEnd -> isPairEnd.toInt) ++ 
                                          Map('filePartNum -> filePartNum.toInt) ++
                                          Map('inFilePath1 -> inFilePath) ++
                                          Map('outFilePath -> outFilePath), list.tail.tail.tail.tail)
        case option :: tail => println("Unknown option " + option) 
                               exit(1) 
      }
    }
    val options = nextOption(Map(),argsList)
    println(options)

    val uploadArgs = new UploadFASTQCommand
    uploadArgs.isPairEnd = options('isPairEnd).toString.toInt
    if(uploadArgs.isPairEnd != 0 && uploadArgs.isPairEnd != 1) {
      println("[Error] Undefined isPairEnd argument" + uploadArgs.isPairEnd)
      exit(1)
    }
    uploadArgs.filePartitionNum = options('filePartNum).toString.toInt  
    uploadArgs.inputFASTQFilePath1 = options('inFilePath1).toString
    if(options.get('inFilePath2) != None)
      uploadArgs.inputFASTQFilePath2 = options('inFilePath2).toString
    uploadArgs.outFileHDFSPath = options('outFilePath).toString
    if(options.get('batchedNum) != None)
      uploadArgs.batchedNum = options('batchedNum).toString.toInt

    println("Upload FASTQ command line arguments: " + uploadArgs.isPairEnd + " " + uploadArgs.filePartitionNum + " " + uploadArgs.inputFASTQFilePath1 + " " 
            + uploadArgs.inputFASTQFilePath2 + " " + uploadArgs.outFileHDFSPath + " " + uploadArgs.batchedNum)
    uploadArgs
  }

 
  private def sortADAMCmdLineParser(argsList: List[String]): List[String] = {
    val parseList: List[String] = argsList match {
      case inputPath :: outputPath :: Nil => List(inputPath, outputPath)
      case _ => println("Unknown command lines arguments")
                exit(1)
    }

    parseList
  }

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
                      "Usage: cs-bwamem [-bfn INT] [-bPSW (0/1)] [-sbatch INT] [-bPSWJNI (0/1)] [-jniPath STRING] [-oType (0/1/2)] [-oPath STRING] [-R STRING] [-isSWExtBatched (0/1)] [-bSWExtSize INT] [-FPGAAcc (0/1)] isPairEnd fastaInputPath fastqHDFSInputPath fastqInputFolderNum\n\n" +
                      "Required arguments (in the following order): \n" +
                      "isPairEnd: perform pair-end (1) or single-end (0) mapping\n" +
                      "fastaInputPath: the path of (local) BWA index files (bns, pac, and so on)\n" +
                      "fastqHDFSInputPath: the path of the raw read files stored in HDFS\n" +
                      "fastqInputFolderNum: the number of folders generated in the HDFS for the raw reads\n\n" +
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
                      "Usage: merge adamHDFSRootInputPath adamHDFSOutputPath\n\n\n" +
                      "Usage 4: sort the output ADAM folder pieces and save as a new ADAM file in HDFS\n" +
                      "Usage: sort adamHDFSRootInputPath adamHDFSOutputPath\n"


  private def commandLineParser(arg: String): String = {
    def getCommand(cmd: String): String = {
      cmd match {
        case "upload-fastq" => cmd
        case "cs-bwamem" => cmd
        case "merge" => cmd
        case "sort" => cmd
        case "cs-bwamem-profile" => cmd
        case "help" => println(usage)
                         exit(1)
        case _ => println("Unknown command " + cmd)
                  println(usage)
                  exit(1)
      }
    }

    val command = getCommand(arg)
    println("command: " + command)

    command
  }

  def main(args: Array[String]) {
    val argsList = args.toList
    val command = commandLineParser(argsList(0))
    var uploadFASTQArgs = new UploadFASTQCommand
    var bwamemArgs = new BWAMEMCommand
    var sortArgs = List[String]()
    val coalesceFactor = 10

    if(command == "upload-fastq") uploadFASTQArgs = uploadFASTQCmdLineParser(argsList.tail)
    else if(command == "cs-bwamem") bwamemArgs = bwamemCmdLineParser(argsList.tail)
    else if(command == "sort" || command == "merge") sortArgs = sortADAMCmdLineParser(argsList.tail)
    else if(command == "cs-bwamem-profile") bwamemArgs = bwamemCmdLineParser(argsList.tail)
    else { 
      println("Unknown command " + command)
      exit(1)
    }
    
    // environment setup
    if(command == "upload-fastq") {
      val conf = new SparkConf().setAppName("Cloud-Scale BWAMEM: upload")
      val sc = new SparkContext(conf)

      val fastqLoader = new FASTQLocalFileLoader(uploadFASTQArgs.batchedNum)
      // single-end upload
      if(uploadFASTQArgs.isPairEnd == 0)
        fastqLoader.storeFASTQInHDFS(sc, uploadFASTQArgs.inputFASTQFilePath1, uploadFASTQArgs.outFileHDFSPath, uploadFASTQArgs.filePartitionNum)
      // pair-end upload
      else
        fastqLoader.storePairEndFASTQInHDFS(sc, uploadFASTQArgs.inputFASTQFilePath1, uploadFASTQArgs.inputFASTQFilePath2, uploadFASTQArgs.outFileHDFSPath, uploadFASTQArgs.filePartitionNum)

      println("Upload FASTQ to HDFS Finished!!!")
    }
    else if(command == "cs-bwamem") {
      val conf = new SparkConf().setAppName("Cloud-Scale BWAMEM: cs-bwamem")
      val sc = new SparkContext(conf)
      
      memMain(sc, bwamemArgs) 
      println("CS-BWAMEM Finished!!!")

      // NOTE: Some of the Spark tasks are in "GET RESULT" status and cause the pending state... 
      //       However, the data are returned. Therefore, we enforce program to exit.
      exit(1)
    }
    else if(command == "cs-bwamem-profile") {
      val conf = new SparkConf().setAppName("Cloud-Scale BWAMEM: cs-bwamem-profile")
      val sc = new SparkContext(conf)
      
      memMainProfile(sc, bwamemArgs) 
      println("CS-BWAMEM Profiling Finished!!!")

      // NOTE: Some of the Spark tasks are in "GET RESULT" status and cause the pending state... 
      //       However, the data are returned. Therefore, we enforce program to exit.
      exit(1)
    }
    else if(command == "merge") {
      val conf = new SparkConf().setAppName("Cloud-Scale BWAMEM: merge").set("spark.scheduler.maxRegisteredResourcesWaitingTime", "600000").set("spark.executor.heartbeatInterval", "100000").set("spark.storage.memoryFraction", "0.7").set("spark.worker.timeout", "300000").set("spark.akka.timeout", "300000").set("spark.storage.blockManagerHeartBeatMs", "300000").set("spark.akka.retry.wait", "300000").set("spark.akka.frameSize", "1000").set("spark.executor.extraLibraryPath", "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.shuffle.consolidateFiles", "true")
      val sc = new SparkContext(conf)

      val adamRecords = MergeADAMFiles(sc, sortArgs(0), coalesceFactor)
      adamRecords.adamSave(sortArgs(1))
    }
    else if(command == "sort") {
      val conf = new SparkConf().setAppName("Cloud-Scale BWAMEM: sort").set("spark.scheduler.maxRegisteredResourcesWaitingTime", "600000").set("spark.executor.heartbeatInterval", "100000").set("spark.storage.memoryFraction", "0.7").set("spark.worker.timeout", "300000").set("spark.akka.timeout", "300000").set("spark.storage.blockManagerHeartBeatMs", "300000").set("spark.akka.retry.wait", "300000").set("spark.akka.frameSize", "1000").set("spark.executor.extraLibraryPath", "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.shuffle.consolidateFiles", "true")
      val sc = new SparkContext(conf)

      val adamRecords = Sort(sc, sortArgs(0), coalesceFactor)
      adamRecords.adamSave(sortArgs(1))
    }

  } 
}

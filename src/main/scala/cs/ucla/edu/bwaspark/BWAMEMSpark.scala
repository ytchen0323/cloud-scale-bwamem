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
import cs.ucla.edu.bwaspark.commandline._

import java.io.FileReader
import java.io.BufferedReader

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
        case "-oSAM" :: value :: tail =>
                               nextOption(map ++ Map('isSAMStringOutput -> value.toInt), tail)
        case "-oSAMPath" :: value :: tail =>
                               nextOption(map ++ Map('samOutputPath -> value), tail)
        case isPairEnd ::  inFASTAPath :: inFASTQPath :: fastqInputFolderNum :: Nil =>  
                               nextOption(map ++ 
                                          Map('isPairEnd -> isPairEnd.toInt) ++ 
                                          Map('inFASTAPath -> inFASTAPath) ++
                                          Map('inFASTQPath -> inFASTQPath) ++
                                          Map('fastqInputFolderNum -> fastqInputFolderNum.toInt), list.tail.tail.tail.tail)
        case option :: tail => println("Unknown option " + option) 
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
        println("Undefined -bPSW argument" + isPSWBatched)
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
        println("Undefined -bPSWJNI argument" + isPSWJNI)
        exit(1)
      }
    }
    if(options.get('jniLibPath) != None)
      bwamemArgs.jniLibPath = options('jniLibPath).toString
    if(options.get('isSAMStringOutput) != None) {
      val isSAMStringOutput = options('isSAMStringOutput).toString.toInt
      if(isSAMStringOutput == 1)
        bwamemArgs.isSAMStringOutput = true
      else if(isSAMStringOutput == 0)
        bwamemArgs.isSAMStringOutput = false
      else {
        println("Undefined -oSAM argument" + isSAMStringOutput)
        exit(1)
      }
    }
    if(options.get('samOutputPath) != None)
      bwamemArgs.samOutputPath = options('samOutputPath).toString

    val isPairEnd = options('isPairEnd).toString.toInt
    if(isPairEnd == 1)
      bwamemArgs.isPairEnd = true
    else if(isPairEnd == 0) 
      bwamemArgs.isPairEnd = false
    else {
      println("Undefined isPairEnd argument" + isPairEnd)
      exit(1)
    }
    bwamemArgs.fastaInputPath = options('inFASTAPath).toString
    bwamemArgs.fastqHDFSInputPath = options('inFASTQPath).toString
    bwamemArgs.fastqInputFolderNum = options('fastqInputFolderNum).toString.toInt  

    println("BWAMEM command line arguments: " + bwamemArgs.isPairEnd + " " + bwamemArgs.fastaInputPath + " " + bwamemArgs.fastqHDFSInputPath + " " + bwamemArgs.fastqInputFolderNum + " " + bwamemArgs.batchedFolderNum + " " + 
            bwamemArgs.isPSWBatched + " " + bwamemArgs.subBatchSize + " " + bwamemArgs.isPSWJNI + " " + bwamemArgs.jniLibPath + " " + bwamemArgs.isSAMStringOutput + " " + bwamemArgs.samOutputPath)

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
      println("Undefined isPairEnd argument" + uploadArgs.isPairEnd)
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


  private def commandLineParser(arg: String): String = {
    def getCommand(cmd: String): String = {
      cmd match {
        case "upload-fastq" => cmd
        case "cs-bwamem" => cmd
        case _ => println("Unknown command " + cmd)
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

    if(command == "upload-fastq") uploadFASTQArgs = uploadFASTQCmdLineParser(argsList.tail)
    else if(command == "cs-bwamem") bwamemArgs = bwamemCmdLineParser(argsList.tail)
    else { 
      println("Unknown command " + command)
      exit(1)
    }
    
    // environment setup
    if(command == "upload-fastq") {
      val conf = new SparkConf().setAppName("Cloud Scale BWAMEM").set("spark.executor.memory", "20g").set("spark.scheduler.maxRegisteredResourcesWaitingTime", "600000").set("spark.executor.heartbeatInterval", "100000").set("spark.storage.memoryFraction", "0.7").set("spark.worker.timeout", "300000").set("spark.akka.threads", "8").set("spark.akka.timeout", "300000").set("spark.storage.blockManagerHeartBeatMs", "300000").set("spark.akka.retry.wait", "300000").set("spark.akka.frameSize", "1024").set("spark.akka.askTimeout", "100").set("spark.logConf", "true").set("spark.executor.extraLibraryPath", "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so")
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
      val conf = new SparkConf().setAppName("Cloud Scale BWAMEM").set("spark.executor.memory", "36g").set("spark.scheduler.maxRegisteredResourcesWaitingTime", "600000").set("spark.executor.heartbeatInterval", "100000").set("spark.storage.memoryFraction", "0.45").set("spark.worker.timeout", "300000").set("spark.akka.timeout", "300000").set("spark.storage.blockManagerHeartBeatMs", "300000").set("spark.akka.retry.wait", "300000").set("spark.akka.frameSize", "10000").set("spark.logConf", "true").set("spark.executor.extraLibraryPath", "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so")
      val sc = new SparkContext(conf)
      
      memMain(sc, bwamemArgs.fastaInputPath, bwamemArgs.fastqHDFSInputPath, bwamemArgs.isPairEnd, bwamemArgs.fastqInputFolderNum, bwamemArgs.batchedFolderNum, bwamemArgs.isPSWBatched, bwamemArgs.subBatchSize, 
              bwamemArgs.isPSWJNI, bwamemArgs.jniLibPath, bwamemArgs.isSAMStringOutput, bwamemArgs.samOutputPath)

      println("CS-BWAMEM Finished!!!")

      // NOTE: Some of the Spark tasks are in "GET RESULT" status and cause the pending state... 
      //       However, the data are returned. Therefore, we enforce program to exit.
      exit(1)
    }

  } 
}

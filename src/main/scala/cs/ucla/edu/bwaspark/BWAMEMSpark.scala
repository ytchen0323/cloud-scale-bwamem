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

import java.io.FileReader
import java.io.BufferedReader

object BWAMEMSpark {
  // load reads from the FASTQ file (for testing use)
  private def loadFASTQSeqs(fileName: String, readNum: Int): Array[String] = {
    
    val reader = new BufferedReader(new FileReader(fileName))
    var line = reader.readLine
    var i = 0
    var readIdx = 0    
    var seqs = new Array[String](readNum / 4)

    while(line != null) {
      if(i % 4 == 1) {
        seqs(readIdx) = line
        readIdx += 1
      }
      i += 1
      line = reader.readLine
    } 

    //seqs.foreach(println(_))
    seqs
  }

  class testRead {
    var seq: String = _
    var regs: MutableList[MemAlnRegType] = new MutableList[MemAlnRegType]
  }

  def main(args: Array[String]) {
    // environment setup
    //val sc = new SparkContext("local[96]", "BWA-mem Spark",
       //"/home/hadoopmaster/spark/spark-0.9.0-incubating-bin-hadoop2-prebuilt/", List("/home/ytchen/incubator/bwa-spark-0.3.1/target/bwa-spark-0.3.1.jar"))
    //val conf = new SparkConf().setAppName("Cloud Scale BWAMEM").set("spark.executor.memory", "36g").set("spark.scheduler.maxRegisteredResourcesWaitingTime", "600000").set("spark.executor.heartbeatInterval", "1000000").set("spark.storage.memoryFraction", "0.45").set("spark.worker.timeout", "300000").set("spark.akka.timeout", "300000").set("spark.storage.blockManagerHeartBeatMs", "300000").set("spark.akka.retry.wait", "300000").set("spark.akka.frameSize", "10000").set("spark.logConf", "true").set("spark.executor.extraLibraryPath", "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so")
    //val conf = new SparkConf().setAppName("Cloud Scale BWAMEM").set("spark.executor.memory", "20g").set("spark.storage.memoryFraction", "0.7").set("spark.akka.frameSize", "10000").set("spark.logConf", "true").set("spark.executor.extraLibraryPath", "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so")
    val conf = new SparkConf().setAppName("Cloud Scale BWAMEM").set("spark.executor.memory", "20g").set("spark.scheduler.maxRegisteredResourcesWaitingTime", "600000").set("spark.executor.heartbeatInterval", "100000").set("spark.storage.memoryFraction", "0.7").set("spark.worker.timeout", "300000").set("spark.akka.threads", "8").set("spark.akka.timeout", "300000").set("spark.storage.blockManagerHeartBeatMs", "300000").set("spark.akka.retry.wait", "300000").set("spark.akka.frameSize", "1024").set("spark.akka.askTimeout", "100").set("spark.logConf", "true").set("spark.executor.extraLibraryPath", "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so")
    val sc = new SparkContext(conf)

    // upload input FASTQ to HDFS
    //val fastqLoader = new FASTQLocalFileLoader(10000000)
    //val fastqLoader = new FASTQLocalFileLoader(100000000)
    //fastqLoader.storeFASTQInHDFS(sc, "/home/pengwei/genomics/InputFiles/HCC1954_1.fq", "hdfs://Jc11:9000/user/ytchen/data/HCC1954_1.fq")
    //fastqLoader.storeFASTQInHDFS(sc, "/home/ytchen/genomics/data/HCC1954_1_10Mreads.fq", "hdfs://Jc11:9000/user/ytchen/data/HCC1954_1.fq")
    //fastqLoader.storePairEndFASTQInHDFS(sc, "/home/ytchen/genomics/data/HCC1954_1_10Mreads.fq", "/home/ytchen/genomics/data/HCC1954_2_10Mreads.fq", "hdfs://Jc11:9000/user/ytchen/data/HCC1954_pair.fq")
    //fastqLoader.storePairEndFASTQInHDFS(sc, "/home/hadoopmaster/genomics/InputFiles/HCC1954_1.fq", "/home/hadoopmaster/genomics/InputFiles/HCC1954_2.fq", "hdfs://Jc11:9000/user/ytchen/data/HCC1954_pair_small_part_size.fq")

/*
    // single-end upload
    val fastqLoader1 = new FASTQLocalFileLoader(250000)
    fastqLoader1.storeFASTQInHDFS(sc, "/home/ytchen/genomics/data/correctness_verification/HCC1954_1_1-1M.fq", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/HCC1954_1_1-1M.fq")
    val fastqLoader2 = new FASTQLocalFileLoader(250000)
    fastqLoader2.storeFASTQInHDFS(sc, "/home/ytchen/genomics/data/correctness_verification/HCC1954_1_10M-11M.fq", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/HCC1954_1_10M-11M.fq")
    val fastqLoader3 = new FASTQLocalFileLoader(250000)
    fastqLoader3.storeFASTQInHDFS(sc, "/home/ytchen/genomics/data/correctness_verification/HCC1954_1_30M-31M.fq", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/HCC1954_1_30M-31M.fq")
    val fastqLoader4 = new FASTQLocalFileLoader(250000)
    fastqLoader4.storeFASTQInHDFS(sc, "/home/ytchen/genomics/data/correctness_verification/HCC1954_1_50M-51M.fq", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/HCC1954_1_50M-51M.fq")
    val fastqLoader5 = new FASTQLocalFileLoader(250000)
    fastqLoader5.storeFASTQInHDFS(sc, "/home/ytchen/genomics/data/correctness_verification/HCC1954_1_80M-81M.fq", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/HCC1954_1_80M-81M.fq")
    val fastqLoader6 = new FASTQLocalFileLoader(250000)
    fastqLoader6.storeFASTQInHDFS(sc, "/home/ytchen/genomics/data/correctness_verification/HCC1954_1_100M-101M.fq", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/HCC1954_1_100M-101M.fq")
*/    

    //val fastqLoader = new FASTQLocalFileLoader(250000)
    //fastqLoader.storePairEndFASTQInHDFS(sc, "/home/hadoopmaster/genomics/InputFiles/HCC1954_1.fq", "/home/hadoopmaster/genomics/InputFiles/HCC1954_2.fq", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/HCC1954.fq")

    val fastqLoader = new FASTQLocalFileLoader(1000000)
    fastqLoader.storeFASTQInHDFS(sc, "/home/ytchen/genomics/data/correctness_verification/HCC1954_1_100M_reads.fq", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/HCC1954_1_100M_reads_1Mpart.fq", 48)
    //val fastqLoader = new FASTQLocalFileLoader(1000000)
    //fastqLoader.storeFASTQInHDFS(sc, "/home/ytchen/genomics/data/correctness_verification/HCC1954_1_10M-14M.fq", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/HCC1954_1_10M-14M_1Mpart.fq")

    // pair-end upload
/*
    val fastqLoader1 = new FASTQLocalFileLoader(250000)
    fastqLoader1.storePairEndFASTQInHDFS(sc, "/home/ytchen/genomics/data/correctness_verification/HCC1954_1_1-4M_permuted.fq", "/home/ytchen/genomics/data/correctness_verification/HCC1954_2_1-4M_permuted.fq", 
      "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/HCC1954_1-4M_permuted.fq")
    val fastqLoader2 = new FASTQLocalFileLoader(250000)
    fastqLoader2.storePairEndFASTQInHDFS(sc, "/home/ytchen/genomics/data/correctness_verification/HCC1954_1_10M-14M_permuted.fq", "/home/ytchen/genomics/data/correctness_verification/HCC1954_2_10M-14M_permuted.fq", 
      "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/HCC1954_10M-14M_permuted.fq")
    val fastqLoader3 = new FASTQLocalFileLoader(250000)
    fastqLoader3.storePairEndFASTQInHDFS(sc, "/home/ytchen/genomics/data/correctness_verification/HCC1954_1_30M-34M_permuted.fq", "/home/ytchen/genomics/data/correctness_verification/HCC1954_2_30M-34M_permuted.fq", 
      "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/HCC1954_30M-34M_permuted.fq")
    val fastqLoader4 = new FASTQLocalFileLoader(250000)
    fastqLoader4.storePairEndFASTQInHDFS(sc, "/home/ytchen/genomics/data/correctness_verification/HCC1954_1_50M-54M_permuted.fq", "/home/ytchen/genomics/data/correctness_verification/HCC1954_2_50M-54M_permuted.fq", 
      "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/HCC1954_50M-54M_permuted.fq")
    val fastqLoader5 = new FASTQLocalFileLoader(250000)
    fastqLoader5.storePairEndFASTQInHDFS(sc, "/home/ytchen/genomics/data/correctness_verification/HCC1954_1_80M-84M_permuted.fq", "/home/ytchen/genomics/data/correctness_verification/HCC1954_2_80M-84M_permuted.fq", 
      "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/HCC1954_80M-84M_permuted.fq")
    val fastqLoader6 = new FASTQLocalFileLoader(250000)
    fastqLoader6.storePairEndFASTQInHDFS(sc, "/home/ytchen/genomics/data/correctness_verification/HCC1954_1_100M-104M_permuted.fq", "/home/ytchen/genomics/data/correctness_verification/HCC1954_2_100M-104M_permuted.fq", 
      "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/HCC1954_100M-104M_permuted.fq")
*/

    // read mapping    
    //memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/HCC1954_10M-11M.fq", true, 9, 4,
            //true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true, "test.sam")
    //memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/HCC1954_all_pair.fq", 401, 4,
            //true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true)
    //memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/HCC1954_pair_small_part_size.fq", 1001, 10,
            //true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true)


    //memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/HCC1954_1_10M-11M.fq", false, 5, 4,
            //true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true, "test_single.sam")

/*
    memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/HCC1954_10M-14M_permuted.fq", true, 33, 32,
            true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true, "HCC1954_10M-14M.sam")
    memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/HCC1954_30M-34M_permuted.fq", true, 33, 32,
            true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true, "HCC1954_30M-34M.sam")
    memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/HCC1954_50M-54M_permuted.fq", true, 33, 32,
            true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true, "HCC1954_50M-54M.sam")
    memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/HCC1954_80M-84M_permuted.fq", true, 33, 32,
            true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true, "HCC1954_80M-84M.sam")
    memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/HCC1954_100M-104M_permuted.fq", true, 33, 32,
            true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true, "HCC1954_100M-104M.sam")
*/

/*
    memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/HCC1954_10M-11M.fq", true, 9, 8,
            true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true, "HCC1954_10M-11M.sam")
    memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/HCC1954_30M-31M.fq", true, 9, 8,
            true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true, "HCC1954_30M-31M.sam")
    memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/HCC1954_50M-51M.fq", true, 9, 8,
            true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true, "HCC1954_50M-51M.sam")
    memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/HCC1954_80M-81M.fq", true, 9, 8,
            true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true, "HCC1954_80M-81M.sam")
    memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/HCC1954_100M-101M.fq", true, 9, 8,
            true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true, "HCC1954_100M-101M.sam")
*/
    //memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/HCC1954_1_10M-11M.fq", false, 5, 4,
            //true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true, "test_single.sam")
//    memMain(sc, "ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/HCC1954_1_10M-11M.fq", false, 5, 4,
//            true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true, "test_single.sam")

    //memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/HCC1954_1_100M_reads.fq", false, 1601, 32,
    //        true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true, "HCC1954_1_100M_reads.sam")

    println("Job Finished!!!")
  } 
}

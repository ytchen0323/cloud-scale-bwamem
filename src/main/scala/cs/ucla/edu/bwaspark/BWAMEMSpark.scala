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
    //val sc = new SparkContext("local[96]", "BWA-mem Spark",
       //"/home/hadoopmaster/spark/spark-0.9.0-incubating-bin-hadoop2-prebuilt/", List("/home/ytchen/incubator/bwa-spark-0.3.1/target/bwa-spark-0.3.1.jar"))
    val conf = new SparkConf().setAppName("Cloud Scale BWAMEM").set("spark.executor.memory", "36g").set("spark.storage.memoryFraction", "0.45").set("spark.akka.frameSize", "128").set("spark.logConf", "true").set("spark.executor.extraLibraryPath", "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so")
    val sc = new SparkContext(conf)

    //val fastqLoader = new FASTQLocalFileLoader(1000000)
    //val fastqLoader = new FASTQLocalFileLoader(2000000)
    //val fastqLoader = new FASTQLocalFileLoader(4000000)
    //val fastqLoader = new FASTQLocalFileLoader(10000000)
    //val fastqLoader = new FASTQLocalFileLoader(40000000)
    //val fastqLoader = new FASTQLocalFileLoader(200000000)
    //fastqLoader.storeFASTQInHDFS(sc, "/home/ytchen/genomics/data/ERR013140_1.filt.fastq", "hdfs://Jc11:9000/user/ytchen/data/ERR013140_1.filt.fastq_96")
    //fastqLoader.storeFASTQInHDFS(sc, "/home/pengwei/genomics/InputFiles/HCC1954_1.fq", "hdfs://Jc11:9000/user/ytchen/data/HCC1954_1.fq")
    //fastqLoader.storeFASTQInHDFS(sc, "/home/ytchen/genomics/data/HCC1954_1_10Mreads.fq", "hdfs://Jc11:9000/user/ytchen/data/HCC1954_1.fq")
    //fastqLoader.storePairEndFASTQInHDFS(sc, "/home/ytchen/genomics/data/HCC1954_1_10Mreads.fq", "/home/ytchen/genomics/data/HCC1954_2_10Mreads.fq", "hdfs://Jc11:9000/user/ytchen/data/HCC1954_pair.fq")
    //fastqLoader.storePairEndFASTQInHDFS(sc, "/home/hadoopmaster/genomics/InputFiles/HCC1954_1.fq", "/home/hadoopmaster/genomics/InputFiles/HCC1954_2.fq", "hdfs://Jc11:9000/user/ytchen/data/HCC1954_pair_small_part_size.fq")
    
    //memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/HCC1954_pair.fq", 3)
    //memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/HCC1954_all_pair.fq", 401)
    memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/HCC1954_all_pair.fq", 401, 4,
            true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true)
    //memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/HCC1954_pair_small_part_size.fq", 1001, 10,
            //true, 10, true, "/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so", true)

    //memMain(sc, "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta", "hdfs://Jc11:9000/user/ytchen/data/HCC1954_all_pair.fq", 401, false, 1000)
/*
    //loading index files
    println("Load Index Files")
    val bwaIdx = new BWAIdxType
    val prefix = "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta"
    bwaIdx.load(prefix, 0)

    //loading BWA MEM options
    println("Load BWA-MEM options")
    val bwaMemOpt = new MemOptType
    bwaMemOpt.load

    //val bwaIdxBWTGlobal = sc.broadcast(bwaIdx.bwt)
    //val bwaIdxBNSGlobal = sc.broadcast(bwaIdx.bns)
    //val bwaIdxPACGlobal = sc.broadcast(bwaIdx.pac)
    //val bwaIdxGlobal = sc.broadcast(bwaIdx)
    val bwaIdxGlobal = sc.broadcast(bwaIdx, prefix)
    val bwaMemOptGlobal = sc.broadcast(bwaMemOpt)

    //debugLevel = 1

    //val fastqRDDLoader = new FASTQRDDLoader(sc, "hdfs://Jc11:9000/user/ytchen/data/HCC1954_1_10Mreads", 2)
    //val fastqRDDLoader = new FASTQRDDLoader(sc, "hdfs://Jc11:9000/user/pengwei/data/HCC1954_1.fq", 201)
    val pairEndFASTQRDDLoader = new FASTQRDDLoader(sc, "hdfs://Jc11:9000/user/ytchen/data/HCC1954_pair.fq", 3)
    val pairEndFASTQRDD = pairEndFASTQRDDLoader.PairEndRDDLoadAll
    //fastqRDD.cache()

// Pair-End
    val reads = pairEndFASTQRDD.map( pairSeq => pairEndBwaMemWorker1(bwaMemOptGlobal.value, bwaIdxGlobal.value.bwt, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, null, pairSeq) )
    val c = reads.count
    println("Pair Count: " + c)

        var k = 0;
        reads.collect.foreach( s => {
          val seq1 = new String(s.seq1.getSeq.array)
          val seq2 = new String(s.seq2.getSeq.array)
          println(k + " Seq1: " + seq1)
          println(k + " Seq2: " + seq2)
          k += 1 } )
*/

// Single-End
/*
    val reads = fastqRDD.map( seq => bwaMemWorker1(bwaMemOptGlobal.value, bwaIdxGlobal.value.bwt, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, null, seq) )
    //val reads = fastqRDD.map( seq => bwaMemWorker1(bwaMemOptGlobal.value, bwaIdxBWTGlobal.value, bwaIdxBNSGlobal.value, bwaIdxPACGlobal.value, null, seq) )
    val c = reads.count
    println("Count: " + c)
    //println("Count: " + reads.map( read => bwaMemWorker2(bwaMemOptGlobal.value, read.regs, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, read.seq, 0) ).reduce(_ + _))
*/    

    println("Job Finished!!!")
  } 
}

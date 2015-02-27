package cs.ucla.edu.bwaspark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import cs.ucla.edu.bwaspark.datatype._
import cs.ucla.edu.bwaspark.worker1.BWAMemWorker1._
import cs.ucla.edu.bwaspark.worker2.BWAMemWorker2._
import cs.ucla.edu.bwaspark.worker2.MemSamPe._
import cs.ucla.edu.bwaspark.sam.SAMHeader
import cs.ucla.edu.bwaspark.sam.SAMWriter
import cs.ucla.edu.bwaspark.sam.SAMHDFSWriter
import cs.ucla.edu.bwaspark.debug.DebugFlag._
import cs.ucla.edu.bwaspark.fastq._
import cs.ucla.edu.bwaspark.util.SWUtil._
import cs.ucla.edu.avro.fastq._

import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.models.{SequenceDictionary, RecordGroup, RecordGroupDictionary}

import htsjdk.samtools.SAMFileHeader

import java.io.FileReader
import java.io.BufferedReader

object FastMap {
  private val MEM_F_PE: Int = 0x2
  private val MEM_F_ALL = 0x8
  private val MEM_F_NO_MULTI = 0x10
  private val packageVersion = "cloud-scale-bwamem-0.1.0"
  private val NO_OUT_FILE = 0
  private val SAM_OUT_FILE = 1
  private val ADAM_OUT = 2

  /**
    *  memMain: the main function to perform read mapping
    *
    *  @param sc the spark context object
    *  @param fastaLocalInputPath the local BWA index files (bns, pac, and so on)
    *  @param fastqHDFSInputPath the raw read file stored in HDFS
    *  @param isPairEnd perform pair-end or single-end mapping
    *  @param fastqInputFolderNum the number of folders generated in the HDFS for the raw reads
    *  @param batchFolderNum the number of raw read folders in a batch to be processed
    *  @param isPSWBatched whether the pair-end Smith Waterman is performed in a batched way
    *  @param subBatchSize the number of reads to be processed in a subbatch
    *  @param isPSWJNI whether the native JNI library is called for better performance
    *  @param jniLibPath the JNI library path in the local machine
    *  @param outputChoice the output format choice
    *  @param outputPath the output path in the local or distributed file system
    */
  def memMain(sc: SparkContext, fastaLocalInputPath: String, fastqHDFSInputPath: String, isPairEnd: Boolean, fastqInputFolderNum: Int, batchFolderNum: Int,
              isPSWBatched: Boolean, subBatchSize: Int, isPSWJNI: Boolean, jniLibPath: String, outputChoice: Int, outputPath: String) {

    val samHeader = new SAMHeader
    var adamHeader = new SequenceDictionary
    val samFileHeader = new SAMFileHeader
    var seqDict: SequenceDictionary = null
    var readGroupDict: RecordGroupDictionary = null
    var readGroup: RecordGroup = null
    val readGroupString: String = "@RG\tID:HCC1954\tLB:HCC1954\tSM:HCC1954"
    val readGroupName = "HCC1954"

    if(samHeader.bwaSetReadGroup("@RG\tID:HCC1954\tLB:HCC1954\tSM:HCC1954")) {
      println("Read line: " + samHeader.readGroupLine)
      println("Read Group ID: " + samHeader.bwaReadGroupID)
    }
    else println("Error on reading header")

    // loading index files
    println("Load Index Files")
    val bwaIdx = new BWAIdxType
    bwaIdx.load(fastaLocalInputPath, 0)

    // loading BWA MEM options
    println("Load BWA-MEM options")
    val bwaMemOpt = new MemOptType
    bwaMemOpt.load

    bwaMemOpt.flag |= MEM_F_ALL
    bwaMemOpt.flag |= MEM_F_NO_MULTI
    
    // write SAM header
    println("Output choice: " + outputChoice)
    var samWriter = new SAMWriter
    if(outputChoice == SAM_OUT_FILE) {
      samWriter.init(outputPath)
      samWriter.writeString(samHeader.bwaGenSAMHeader(bwaIdx.bns, packageVersion))
    }
    else if(outputChoice == ADAM_OUT) {
      samHeader.bwaGenSAMHeader(bwaIdx.bns, packageVersion, readGroupString, samFileHeader)
      seqDict = SequenceDictionary(samFileHeader)
      readGroupDict = RecordGroupDictionary.fromSAMHeader(samFileHeader)
      readGroup = readGroupDict(readGroupName)
    }

    // pair-end read mapping
    if(isPairEnd) {
      bwaMemOpt.flag |= MEM_F_PE
      if(outputChoice == SAM_OUT_FILE)
        memPairEndMapping(sc, fastaLocalInputPath, fastqHDFSInputPath, fastqInputFolderNum, batchFolderNum, bwaMemOpt, bwaIdx, 
                          isPSWBatched, subBatchSize, isPSWJNI, jniLibPath, outputChoice, samWriter, outputPath, samHeader)
      else if(outputChoice == ADAM_OUT)
        memPairEndMapping(sc, fastaLocalInputPath, fastqHDFSInputPath, fastqInputFolderNum, batchFolderNum, bwaMemOpt, bwaIdx, 
                          isPSWBatched, subBatchSize, isPSWJNI, jniLibPath, outputChoice, samWriter, outputPath, samHeader, seqDict, readGroup)
        
    }
    // single-end read mapping
    else {
      if(outputChoice == SAM_OUT_FILE)
        memSingleEndMapping(sc, fastaLocalInputPath, fastqHDFSInputPath, fastqInputFolderNum, batchFolderNum, bwaMemOpt, bwaIdx, outputChoice, samWriter, outputPath, samHeader)
      else if(outputChoice == ADAM_OUT)
        memSingleEndMapping(sc, fastaLocalInputPath, fastqHDFSInputPath, fastqInputFolderNum, batchFolderNum, bwaMemOpt, bwaIdx, outputChoice, samWriter, outputPath, samHeader, seqDict, readGroup)
    }

    if(outputChoice == SAM_OUT_FILE) 
      samWriter.close
  } 


  /**
    *  memSingleEndMapping: the main function to perform single-end read mapping
    *
    *  @param sc the spark context object
    *  @param fastaLocalInputPath the local BWA index files (bns, pac, and so on)
    *  @param fastqHDFSInputPath the raw read file stored in HDFS
    *  @param fastqInputFolderNum the number of folders generated in the HDFS for the raw reads
    *  @param batchFolderNum the number of raw read folders in a batch to be processed
    *  @param bwaMemOpt the MemOptType object
    *  @param bwaIdx the BWAIdxType object
    *  @param outputChoice the output format choice
    *  @param samWriter the writer to write SAM file at the local file system
    *  @param outputPath the output path in the local or distributed file system
    *  @param samHeader the SAM header file used for writing SAM output file
    *  @param seqDict (optional) the sequences (chromosome) dictionary: used for ADAM format output
    *  @param readGroup (optional) the read group: used for ADAM format output
    */
  private def memSingleEndMapping(sc: SparkContext, fastaLocalInputPath: String, fastqHDFSInputPath: String, fastqInputFolderNum: Int, batchFolderNum: Int, 
                                  bwaMemOpt: MemOptType, bwaIdx: BWAIdxType, outputChoice: Int, samWriter: SAMWriter, outputPath: String, samHeader: SAMHeader,
                                  seqDict: SequenceDictionary = null, readGroup: RecordGroup = null) 
  {

    // broadcast shared variables
    val bwaIdxGlobal = sc.broadcast(bwaIdx, fastaLocalInputPath)  // read from local disks!!!
    val bwaMemOptGlobal = sc.broadcast(bwaMemOpt)
    val fastqRDDLoader = new FASTQRDDLoader(sc, fastqHDFSInputPath, fastqInputFolderNum)

    // Not output SAM file
    if(outputChoice == NO_OUT_FILE) {
      // loading reads
      println("Load FASTQ files")
      val fastqRDD = fastqRDDLoader.RDDLoadAll

      println("@Worker1")
      val reads = fastqRDD.map( seq => bwaMemWorker1(bwaMemOptGlobal.value, bwaIdxGlobal.value.bwt, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, null, seq) )
      println("@Worker2")
      val c = reads.map( r => singleEndBwaMemWorker2(bwaMemOptGlobal.value, r.regs, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, r.seq, 0, samHeader) ).count
      println("Count: " + c)
    }
    // output SAM file
    else if(outputChoice == SAM_OUT_FILE) {
      var numProcessed: Long = 0

      // Process the reads in a batched fashion
      var i: Int = 0
      while(i < fastqInputFolderNum) {
        val restFolderNum = fastqInputFolderNum - i
        var singleEndFASTQRDD: RDD[FASTQRecord] = null
        if(restFolderNum >= batchFolderNum) {
          singleEndFASTQRDD = fastqRDDLoader.SingleEndRDDLoadOneBatch(i, batchFolderNum)
          i += batchFolderNum
        }
        else {
          singleEndFASTQRDD = fastqRDDLoader.SingleEndRDDLoadOneBatch(i, restFolderNum)
          i += restFolderNum
        }

        // worker1, worker2, and return SAM format strings
        val samStrings = singleEndFASTQRDD.map(seq => bwaMemWorker1(bwaMemOptGlobal.value, bwaIdxGlobal.value.bwt, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, null, seq) )
                                          .map(r => singleEndBwaMemWorker2(bwaMemOptGlobal.value, r.regs, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, r.seq, numProcessed, samHeader) )
                                          .collect

        numProcessed += samStrings.size

        // Write to the output file in a sequencial way (for now)
        samWriter.writeStringArray(samStrings)
      }
    } 
    // output ADAM format to the distributed file system
    else if(outputChoice == ADAM_OUT) {
      var numProcessed: Long = 0

      // Used to avoid time consuming adamRDD.count (numProcessed += adamRDD.count)
      // Assume the number of read in one batch is the same (This is determined when uploading FASTQ to HDFS)
      val fastqRDDLoaderTmp = new FASTQRDDLoader(sc, fastqHDFSInputPath, fastqInputFolderNum)
      val rddTmp = fastqRDDLoaderTmp.SingleEndRDDLoadOneBatch(0, batchFolderNum)
      val batchedReadNum = rddTmp.count
      rddTmp.unpersist(true)

      // Process the reads in a batched fashion
      var i: Int = 0
      var folderID: Int = 0
      while(i < fastqInputFolderNum) {
        val restFolderNum = fastqInputFolderNum - i
        var singleEndFASTQRDD: RDD[FASTQRecord] = null
        if(restFolderNum >= batchFolderNum) {
          singleEndFASTQRDD = fastqRDDLoader.SingleEndRDDLoadOneBatch(i, batchFolderNum)
          i += batchFolderNum
        }
        else {
          singleEndFASTQRDD = fastqRDDLoader.SingleEndRDDLoadOneBatch(i, restFolderNum)
          i += restFolderNum
        }

        // worker1, worker2, and return SAM format strings
        val adamRDD = singleEndFASTQRDD.map(seq => bwaMemWorker1(bwaMemOptGlobal.value, bwaIdxGlobal.value.bwt, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, null, seq) )
                                       .flatMap(r => singleEndBwaMemWorker2ADAMOut(bwaMemOptGlobal.value, r.regs, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, 
                                                                                   r.seq, numProcessed, samHeader, seqDict, readGroup) )
                                          
        adamRDD.adamSave(outputPath + "/"  + folderID.toString())
        numProcessed += batchedReadNum
        folderID += 1
      }
    }
    else {
      println("[Error] Undefined output choice" + outputChoice)
      exit(1)
    }

  }


  /**
    *  memPairEndMapping: the main function to perform pair-end read mapping
    *
    *  @param sc the spark context object
    *  @param fastaLocalInputPath the local BWA index files (bns, pac, and so on)
    *  @param fastqHDFSInputPath the raw read file stored in HDFS
    *  @param fastqInputFolderNum the number of folders generated in the HDFS for the raw reads
    *  @param batchFolderNum the number of raw read folders in a batch to be processed
    *  @param bwaMemOpt the MemOptType object
    *  @param bwaIdx the BWAIdxType object
    *  @param isPSWBatched whether the pair-end Smith Waterman is performed in a batched way
    *  @param subBatchSize the number of reads to be processed in a subbatch
    *  @param isPSWJNI whether the native JNI library is called for better performance
    *  @param jniLibPath the JNI library path in the local machine
    *  @param outputChoice the output format choice
    *  @param samWriter the writer to write SAM file at the local file system
    *  @param outputPath the output path in the local or distributed file system
    *  @param samHeader the SAM header file used for writing SAM output file
    *  @param seqDict (optional) the sequences (chromosome) dictionary: used for ADAM format output
    *  @param readGroup (optional) the read group: used for ADAM format output
    */
  private def memPairEndMapping(sc: SparkContext, fastaLocalInputPath: String, fastqHDFSInputPath: String, fastqInputFolderNum: Int, batchFolderNum: Int, 
                                bwaMemOpt: MemOptType, bwaIdx: BWAIdxType, isPSWBatched: Boolean, subBatchSize: Int, isPSWJNI: Boolean, jniLibPath: String, 
                                outputChoice: Int, samWriter: SAMWriter, outputPath: String, samHeader: SAMHeader, seqDict: SequenceDictionary = null, readGroup: RecordGroup = null) 
  {

    // broadcast shared variables
    val bwaIdxGlobal = sc.broadcast(bwaIdx, fastaLocalInputPath)  // read from local disks!!!
    val bwaMemOptGlobal = sc.broadcast(bwaMemOpt)

    // Used to avoid time consuming adamRDD.count (numProcessed += adamRDD.count)
    // Assume the number of read in one batch is the same (This is determined when uploading FASTQ to HDFS)
    val fastqRDDLoaderTmp = new FASTQRDDLoader(sc, fastqHDFSInputPath, fastqInputFolderNum)
    val rddTmp = fastqRDDLoaderTmp.PairEndRDDLoadOneBatch(0, batchFolderNum)
    val batchedReadNum = rddTmp.count
    rddTmp.unpersist(true)

    var numProcessed: Long = 0
    // Process the reads in a batched fashion
    var i: Int = 0
    var folderID: Int = 0
    while(i < fastqInputFolderNum) {
      
      var pes: Array[MemPeStat] = new Array[MemPeStat](4)
      var j = 0
      while(j < 4) {
        pes(j) = new MemPeStat
        j += 1
      }

      // loading reads
      println("Load FASTQ files")
      val pairEndFASTQRDDLoader = new FASTQRDDLoader(sc, fastqHDFSInputPath, fastqInputFolderNum)
      val restFolderNum = fastqInputFolderNum - i
      var pairEndFASTQRDD: RDD[PairEndFASTQRecord] = null
      if(restFolderNum >= batchFolderNum) {
        pairEndFASTQRDD = pairEndFASTQRDDLoader.PairEndRDDLoadOneBatch(i, batchFolderNum)
        i += batchFolderNum
      }
      else {
        pairEndFASTQRDD = pairEndFASTQRDDLoader.PairEndRDDLoadOneBatch(i, restFolderNum)
        i += restFolderNum
      }

      // Worker1 (Map step)
      println("@Worker1")
      val reads = pairEndFASTQRDD.map( pairSeq => pairEndBwaMemWorker1(bwaMemOptGlobal.value, bwaIdxGlobal.value.bwt, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, null, pairSeq) ) 
      reads.cache

      // MemPeStat (Reduce step)
      val peStatPrepRDD = reads.map( pairSeq => memPeStatPrep(bwaMemOptGlobal.value, bwaIdxGlobal.value.bns.l_pac, pairSeq) )
      val peStatPrepArray = peStatPrepRDD.collect
      memPeStatCompute(bwaMemOptGlobal.value, peStatPrepArray, pes)

      println("@MemPeStat")
      j = 0
      while(j < 4) {
        println("pes(" + j + "): " + pes(j).low + " " + pes(j).high + " " + pes(j).failed + " " + pes(j).avg + " " + pes(j).std)
        j += 1
      }
        
      // Worker2 (Map step)
      // NOTE: we may need to find how to utilize the numProcessed variable!!!
      // Batched Processing for P-SW kernel
      if(isPSWBatched) {
        // Not output SAM format file
        if(outputChoice == NO_OUT_FILE) {
          def it2ArrayIt(iter: Iterator[PairEndReadType]): Iterator[Unit] = {
            var counter = 0
            var ret: Vector[Unit] = scala.collection.immutable.Vector.empty
            var subBatch = new Array[PairEndReadType](subBatchSize)
            while (iter.hasNext) {
              subBatch(counter) = iter.next
              counter = counter + 1
              if (counter == subBatchSize) {
                ret = ret :+ pairEndBwaMemWorker2PSWBatched(bwaMemOptGlobal.value, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, 0, pes, subBatch, subBatchSize, isPSWJNI, jniLibPath, samHeader) 
                counter = 0
              }
            }
            if (counter != 0)
              ret = ret :+ pairEndBwaMemWorker2PSWBatched(bwaMemOptGlobal.value, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, 0, pes, subBatch, counter, isPSWJNI, jniLibPath, samHeader)
            ret.toArray.iterator
          }

          val count = reads.mapPartitions(it2ArrayIt).count
          println("Count: " + count)
 
          reads.unpersist(true)   // free RDD; seems to be needed (free storage information is wrong)
        }
        // Output SAM format file
        else if(outputChoice == SAM_OUT_FILE) {
          def it2ArrayIt(iter: Iterator[PairEndReadType]): Iterator[Array[Array[String]]] = {
            var counter = 0
            var ret: Vector[Array[Array[String]]] = scala.collection.immutable.Vector.empty
            var subBatch = new Array[PairEndReadType](subBatchSize)
            while (iter.hasNext) {
              subBatch(counter) = iter.next
              counter = counter + 1
              if (counter == subBatchSize) {
                ret = ret :+ pairEndBwaMemWorker2PSWBatchedSAMRet(bwaMemOptGlobal.value, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, 0, pes, subBatch, subBatchSize, isPSWJNI, jniLibPath, samHeader) 
                counter = 0
              }
            }
            if (counter != 0)
              ret = ret :+ pairEndBwaMemWorker2PSWBatchedSAMRet(bwaMemOptGlobal.value, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, 0, pes, subBatch, counter, isPSWJNI, jniLibPath, samHeader)
            ret.toArray.iterator
          }
 
          val samStrings = reads.mapPartitions(it2ArrayIt).collect
          println("Count: " + samStrings.size)
          reads.unpersist(true)   // free RDD; seems to be needed (free storage information is wrong)
 
          // Write to the output file in a sequencial way (for now)
          samStrings.foreach(s => {
            s.foreach(pairSeq => {
              samWriter.writeString(pairSeq(0))
              samWriter.writeString(pairSeq(1))
            } )
          } )
  
        }
        // Output ADAM format file
        else if(outputChoice == ADAM_OUT) {
          def it2ArrayIt(iter: Iterator[PairEndReadType]): Iterator[Array[AlignmentRecord]] = {
            var counter = 0
            var ret: Vector[Array[AlignmentRecord]] = scala.collection.immutable.Vector.empty
            var subBatch = new Array[PairEndReadType](subBatchSize)
            while (iter.hasNext) {
              subBatch(counter) = iter.next
              counter = counter + 1
              if (counter == subBatchSize) {
                ret = ret :+ pairEndBwaMemWorker2PSWBatchedADAMRet(bwaMemOptGlobal.value, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, 0, pes, subBatch, subBatchSize, isPSWJNI, jniLibPath, samHeader, seqDict, readGroup) 
                counter = 0
              }
            }
            if (counter != 0)
              ret = ret :+ pairEndBwaMemWorker2PSWBatchedADAMRet(bwaMemOptGlobal.value, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, 0, pes, subBatch, counter, isPSWJNI, jniLibPath, samHeader, seqDict, readGroup)
            ret.toArray.iterator
          }
 
          //val adamObjRDD = sc.union(reads.mapPartitions(it2ArrayIt))
          val adamObjRDD = reads.mapPartitions(it2ArrayIt).flatMap(r => r)
          adamObjRDD.adamSave(outputPath + "/"  + folderID.toString())
          numProcessed += batchedReadNum
          folderID += 1
          reads.unpersist(true)
          adamObjRDD.unpersist(true)  // free RDD; seems to be needed (free storage information is wrong)         
        }
      }
      // NOTE: need to be modified!!!
      // Normal read-based processing
      else {
        val count = reads.map(pairSeq => pairEndBwaMemWorker2(bwaMemOptGlobal.value, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, 0, pes, pairSeq, samHeader) ).count
        numProcessed += count.toLong
      }
 
    }

  }

} 

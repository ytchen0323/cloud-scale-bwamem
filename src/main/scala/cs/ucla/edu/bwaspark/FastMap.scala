package cs.ucla.edu.bwaspark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import cs.ucla.edu.bwaspark.datatype._
import cs.ucla.edu.bwaspark.worker1.BWAMemWorker1._
import cs.ucla.edu.bwaspark.worker2.BWAMemWorker2._
import cs.ucla.edu.bwaspark.worker2.MemSamPe._
import cs.ucla.edu.bwaspark.sam.SAMHeader._
import cs.ucla.edu.bwaspark.sam.SAMWriter
import cs.ucla.edu.bwaspark.debug.DebugFlag._
import cs.ucla.edu.bwaspark.fastq._
import cs.ucla.edu.bwaspark.util.SWUtil._

// for test use
import cs.ucla.edu.bwaspark.worker2.MemRegToADAMSAM._

import java.io.FileReader
import java.io.BufferedReader

object FastMap {
  private val MEM_F_PE: Int = 0x2
  private val MEM_F_ALL = 0x8
  private val MEM_F_NO_MULTI = 0x10


  def memMain(sc: SparkContext, fastaLocalInputPath: String, fastqHDFSInputPath: String, fastqInputFolderNum: Int) {

    if(bwaSetReadGroup("@RG\tID:HCC1954\tLB:HCC1954\tSM:HCC1954")) {
      println("Read line: " + readGroupLine)
      println("Read Group ID: " + bwaReadGroupID)
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

    // set the single/pair end mode
    // pair-end setting now!
    bwaMemOpt.flag |= MEM_F_PE
    bwaMemOpt.flag |= MEM_F_ALL
    bwaMemOpt.flag |= MEM_F_NO_MULTI
    
    // broadcast shared variables
    val bwaIdxGlobal = sc.broadcast(bwaIdx, fastaLocalInputPath)  // read from local disks!!!
    val bwaMemOptGlobal = sc.broadcast(bwaMemOpt)

//    var batchNum = 99010
    var n: Int = 0
//    var numProcessed = 0
//    var readNum = 0

    // write SAM header
    //val samWriter = new SAMWriter("test.sam")
    //samWriter.init
    //samWriter.writeString(bwaGenSAMHeader(bwaIdx.bns))

// Do only one iteration for now
//    var i: Int = 0
//    while(i < fastqInputFolderNum) {

      if((bwaMemOpt.flag & MEM_F_PE) > 0) {
        var pes: Array[MemPeStat] = new Array[MemPeStat](4)
        var j = 0
        while(j < 4) {
          pes(j) = new MemPeStat
          j += 1
        }

        // loading reads
        println("Load FASTQ files")
        val pairEndFASTQRDDLoader = new FASTQRDDLoader(sc, fastqHDFSInputPath, fastqInputFolderNum)
        //var path = fastqHDFSInputPath + "/" + i.toString
        //val pairEndFASTQRDD = pairEndFASTQRDDLoader.PairEndRDDLoad(path)
        val pairEndFASTQRDD = pairEndFASTQRDDLoader.PairEndRDDLoadAll

        // worker1
        println("@Worker1")
        val reads = pairEndFASTQRDD.map( pairSeq => pairEndBwaMemWorker1(bwaMemOptGlobal.value, bwaIdxGlobal.value.bwt, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, null, pairSeq) )      
        
        // collect RDD
        //val regsAllReads = reads.toArray
        //n = regsAllReads.size
        //println("Pair-End Read Count: " + n)

        val c = reads.count
        println("Pair-End Read Count: " + c)
        val peStatPrepRDD = reads.map( pairSeq => memPeStatPrep(bwaMemOptGlobal.value, bwaIdxGlobal.value.bns.l_pac, pairSeq) )
        val peStatPrepArray = peStatPrepRDD.collect
        memPeStatCompute(bwaMemOptGlobal.value, peStatPrepArray, pes)

        println("@MemPeStat")
        j = 0
        while(j < 4) {
          println("pes(" + j + "): " + pes(j).low + " " + pes(j).high + " " + pes(j).failed + " " + pes(j).avg + " " + pes(j).std)
          j += 1
        }

        reads.map(pairSeq => pairEndBwaMemWorker2(bwaMemOptGlobal.value, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, 0, pes, pairSeq) ).count

        // testing
        //var k = 0;
        //peStatPrepRDD.collect.foreach( s => {
          //println(k + " dir: " + s.dir + " dist: " + s.dist)
          //k += 1 } )

        // repartition
        //val readsRepart = reads.coalesce(1, false)
        //val re = readsRepart.count
        //println("After repartition count: " + re)

/*
        var k = 0;
        reads.collect.foreach( s => {
          val seq1 = new String(s.seq1.getSeq.array)
          val seq2 = new String(s.seq2.getSeq.array)
          println(k + " Seq1: " + seq1)
          println(k + " Seq2: " + seq2) 
          k += 1 } )
*/
        // memPeStat
        //memPeStat(bwaMemOptGlobal.value, bwaIdxGlobal.value.bns.l_pac, n, regsAllReads, pes)
      }
      // need to be modified
      else {
        // loading reads
        println("Load FASTQ files")
        val fastqRDDLoader = new FASTQRDDLoader(sc, fastqHDFSInputPath, fastqInputFolderNum)
        val fastqRDD = fastqRDDLoader.RDDLoadAll

        println("@Worker1")
        val reads = fastqRDD.map( seq => bwaMemWorker1(bwaMemOptGlobal.value, bwaIdxGlobal.value.bwt, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, null, seq) )
        val c = reads.count
        println("Count: " + c)
      }

      // Debugging
/*
      n = seqs.size
      
      var bpNum: Long = 0
      var i = 0   
      while(i < n) {
        bpNum += seqs(i).seqLen
        i += 1
      }
 
      println("read " + n + " sequences (" + bpNum + " bp)")

      //seqs.foreach(s => println(s.seq))

      memProcessSeqs(bwaMemOpt, bwaIdx.bwt, bwaIdx.bns, bwaIdx.pac, numProcessed, n, seqs, null, samWriter)
      numProcessed += n
      
      println("Num processed: " + numProcessed)
*/
//      i += 1
//      println("i: " + i)
//    } 

    //samWriter.close
  } 

/*
  def memProcessSeqs(opt: MemOptType, bwt: BWTType, bns: BNTSeqType, pac: Array[Byte], numProcessed: Long, n: Int, seqs: Array[FASTQSingleNode], pes0: Array[MemPeStat], samWriter: SAMWriter) {

    var pes: Array[MemPeStat] = new Array[MemPeStat](4)   
    var j = 0
    while(j < 4) {
      pes(j) = new MemPeStat
      j += 1
    }

    // worker1
    // find mapping positions
    println("@Worker1")
    var i = 0
    var regsAllReads: Array[MemAlnRegArrayType] = new Array[MemAlnRegArrayType](seqs.length)

    if((opt.flag & MEM_F_PE) == 0) {
      while(i < seqs.length) {
        regsAllReads(i) = bwaMemWorker1(opt, bwt, bns, pac, seqs(i).seqLen, seqs(i).seq)
        i += 1
        if((i % 10000) == 0) println(i)
      }
    }
    else {
      while(i < seqs.length) {
        regsAllReads(i) = bwaMemWorker1(opt, bwt, bns, pac, seqs(i).seqLen, seqs(i).seq)
        i += 1
        regsAllReads(i) = bwaMemWorker1(opt, bwt, bns, pac, seqs(i).seqLen, seqs(i).seq)
        i += 1

        if((i % 10000) == 0) println(i)
      }
    }

    var testReads = new Array[testRead](seqs.length)
    i = 0
    while(i < seqs.length) {
      var read = new testRead
      read.seq = seqs(i)
      read.regs = regsAllReads(i)
      testReads(i) = read
      i += 1
    }

   
    println("@MemPeStat")
    if((opt.flag & MEM_F_PE) > 0) { // infer insert sizes if not provided
      if(pes0 != null) // if pes0 != NULL, set the insert-size distribution as pes0
        pes = pes0
      else // otherwise, infer the insert size distribution from data
        memPeStat(opt, bns.l_pac, n, regsAllReads, pes)
    }    

    println("@Worker2")
    i = 0
    if((opt.flag & MEM_F_PE) == 0) {
      testReads.foreach(read => {
        bwaMemWorker2(opt, read.regs.regs, bns, pac, read.seq, 0) 
        i += 1
        if((i % 10000) == 0) println(i) } )
    }
    else {

      while(i < testReads.size) {
        var alnRegVec: Array[Array[MemAlnRegType]] = new Array[Array[MemAlnRegType]](2)
        var seqs: Array[FASTQSingleNode] = new Array[FASTQSingleNode](2)
      
        if(testReads(i).regs != null) 
          alnRegVec(0) = testReads(i).regs.regs
        seqs(0) = testReads(i).seq
        if(testReads(i + 1).regs != null) 
          alnRegVec(1) = testReads(i + 1).regs.regs
        seqs(1) = testReads(i + 1).seq
        bwaMemWorker2Pair(opt, alnRegVec, bns, pac, seqs, (numProcessed + i) >> 1, pes)      

        i += 2
        if((i % 1000) == 0) println(i)
      }
    }

// Batched
//////
      var seqsPairs: Array[Array[FASTQSingleNode]] = new Array[Array[FASTQSingleNode]](testReads.size >> 1)
      var alnRegVecPairs: Array[Array[Array[MemAlnRegType]]] = new Array[Array[Array[MemAlnRegType]]](testReads.size >> 1)
      var idx = 0

      while(i < testReads.size) {
        var alnRegVec: Array[Array[MemAlnRegType]] = new Array[Array[MemAlnRegType]](2)
        var seqs: Array[FASTQSingleNode] = new Array[FASTQSingleNode](2)
      
        if(testReads(i).regs != null) 
          alnRegVec(0) = testReads(i).regs.regs
        seqs(0) = testReads(i).seq
        if(testReads(i + 1).regs != null) 
          alnRegVec(1) = testReads(i + 1).regs.regs
        seqs(1) = testReads(i + 1).seq

        seqsPairs(idx) = new Array[FASTQSingleNode](2)
        seqsPairs(idx)(0) = seqs(0)
        seqsPairs(idx)(1) = seqs(1)
        alnRegVecPairs(idx) = new Array[Array[MemAlnRegType]](2)
        alnRegVecPairs(idx)(0) = alnRegVec(0)
        alnRegVecPairs(idx)(1) = alnRegVec(1)

        idx += 1
        i += 2
        //if((i % 1000) == 0) println(i)
      }

      memSamPeGroup(opt, bns, pac, pes, testReads.size >> 1, numProcessed >> 1, seqsPairs, alnRegVecPairs)

    }
//////
    //testReads.foreach(r => samWriter.writeString((r.seq.sam)))

  }
*/
/*
  def memMainJNI() {

    if(bwaSetReadGroup("@RG\tID:HCC1954\tLB:HCC1954\tSM:HCC1954")) {
      println("Read line: " + readGroupLine)
      println("Read Group ID: " + bwaReadGroupID)
    }
    else println("Error on reading header")

    //loading index files
    println("Load Index Files")
    val bwaIdx = new BWAIdxType
    val prefix = "/home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta"
    bwaIdx.load(prefix, 0)

    //loading BWA MEM options
    println("Load BWA-MEM options")
    val bwaMemOpt = new MemOptType
    bwaMemOpt.load

    //loading reads
    println("Load FASTQ files")

    // set the single/pair end mode
    bwaMemOpt.flag |= MEM_F_PE
    bwaMemOpt.flag |= MEM_F_ALL
    bwaMemOpt.flag |= MEM_F_NO_MULTI

    // Set readNum to the total number of reads in JNI mode
    //var readNum = 5000000
    var readNum = 1000000
    var batchNum = 99010
    var n = 0
    var numProcessed = 0
    var seqs: Array[FASTQSingleNode] = new Array[FASTQSingleNode](0)
    var reader1: BufferedReader = null
    var reader2: BufferedReader = null

    if((bwaMemOpt.flag & MEM_F_PE) > 0) {
      //reader1 = new BufferedReader(new FileReader("/home/ytchen/genomics/data/HCC1954_1_10Mreads.fq"))
      //reader2 = new BufferedReader(new FileReader("/home/ytchen/genomics/data/HCC1954_2_10Mreads.fq"))
      reader1 = new BufferedReader(new FileReader("/home/ytchen/genomics/data/JNI_1_2M.fq"))
      reader2 = new BufferedReader(new FileReader("/home/ytchen/genomics/data/JNI_2_2M.fq"))
    }
    else
      reader1 = new BufferedReader(new FileReader("/home/ytchen/genomics/data/HCC1954_1_20reads.fq"))      

    val samWriter = new SAMWriter("test.sam")
    samWriter.init
    samWriter.writeString(bwaGenSAMHeader(bwaIdx.bns))

    println("Read FASTQ")
    if((bwaMemOpt.flag & MEM_F_PE) > 0)
      seqs = loadBatchPairFASTQSeqs(reader1, reader2, readNum)
    else      
      seqs = loadBatchFASTQSeqs(reader1, readNum / 2)

    do {
      if(readNum - numProcessed >= batchNum) n = batchNum
      else n = readNum - numProcessed

      var seqsInBatch = new Array[FASTQSingleNode](n)
      
      var bpNum: Long = 0
      var i = 0 
      println("[JNI] Read Starting Index: " + (i + numProcessed)); 
      while(i < n) {
        bpNum += seqs(i).seqLen
        seqsInBatch(i) = seqs(i + numProcessed)
        i += 1
      }
 
      println("read " + n + " sequences (" + bpNum + " bp)")

      memProcessSeqsJNI(bwaMemOpt, bwaIdx.bwt, bwaIdx.bns, bwaIdx.pac, numProcessed, n, seqsInBatch, null, samWriter)
      numProcessed += n
      
      println("Num processed: " + numProcessed)

    } while(n == batchNum)
    
    if((bwaMemOpt.flag & MEM_F_PE) > 0) {
      reader1.close
      reader2.close
    }
    else
      reader1.close
    samWriter.close
  } 

  def memProcessSeqsJNI(opt: MemOptType, bwt: BWTType, bns: BNTSeqType, pac: Array[Byte], numProcessed: Long, n: Int, seqs: Array[FASTQSingleNode], pes0: Array[MemPeStat], samWriter: SAMWriter) {

    var pes: Array[MemPeStat] = new Array[MemPeStat](4)   
    var j = 0
    while(j < 4) {
      pes(j) = new MemPeStat
      j += 1
    }


    // worker1
    // find mapping positions
    println("@Worker1")
    var i = 0
    var regsAllReads: Array[MemAlnRegArrayType] = new Array[MemAlnRegArrayType](seqs.length)

    if((opt.flag & MEM_F_PE) == 0) {
      while(i < seqs.length) {
        regsAllReads(i) = bwaMemWorker1(opt, bwt, bns, pac, seqs(i).seqLen, seqs(i).seq)
        i += 1
        if((i % 10000) == 0) println(i)
      }
    }
    else {
      while(i < seqs.length) {
        regsAllReads(i) = bwaMemWorker1(opt, bwt, bns, pac, seqs(i).seqLen, seqs(i).seq)
        i += 1
        regsAllReads(i) = bwaMemWorker1(opt, bwt, bns, pac, seqs(i).seqLen, seqs(i).seq)
        i += 1

        if((i % 10000) == 0) println(i)
      }
    }

    var testReads = new Array[testRead](seqs.length)
    i = 0
    while(i < seqs.length) {
      var read = new testRead
      read.seq = seqs(i)
      read.regs = regsAllReads(i)
      testReads(i) = read
      i += 1
    }

   
    println("@MemPeStat")
    if((opt.flag & MEM_F_PE) > 0) { // infer insert sizes if not provided
      if(pes0 != null) // if pes0 != NULL, set the insert-size distribution as pes0
        pes = pes0
      else // otherwise, infer the insert size distribution from data
        memPeStat(opt, bns.l_pac, n, regsAllReads, pes)
    }    

    println("@Worker2")
    i = 0
    if((opt.flag & MEM_F_PE) == 0) {
      testReads.foreach(read => {
        bwaMemWorker2(opt, read.regs.regs, bns, pac, read.seq, 0) 
        i += 1
        if((i % 10000) == 0) println(i) } )
    }
    else {
      var seqsPairs: Array[Array[FASTQSingleNode]] = new Array[Array[FASTQSingleNode]](testReads.size >> 1)
      var alnRegVecPairs: Array[Array[Array[MemAlnRegType]]] = new Array[Array[Array[MemAlnRegType]]](testReads.size >> 1)
      var idx = 0

      while(i < testReads.size) {
        var alnRegVec: Array[Array[MemAlnRegType]] = new Array[Array[MemAlnRegType]](2)
        var seqs: Array[FASTQSingleNode] = new Array[FASTQSingleNode](2)
      
        if(testReads(i).regs != null) 
          alnRegVec(0) = testReads(i).regs.regs
        seqs(0) = testReads(i).seq
        if(testReads(i + 1).regs != null) 
          alnRegVec(1) = testReads(i + 1).regs.regs
        seqs(1) = testReads(i + 1).seq

        seqsPairs(idx) = new Array[FASTQSingleNode](2)
        seqsPairs(idx)(0) = seqs(0)
        seqsPairs(idx)(1) = seqs(1)
        alnRegVecPairs(idx) = new Array[Array[MemAlnRegType]](2)
        alnRegVecPairs(idx)(0) = alnRegVec(0)
        alnRegVecPairs(idx)(1) = alnRegVec(1)

        idx += 1
        i += 2
      }

      memSamPeGroupJNI(opt, bns, pac, pes, testReads.size >> 1, numProcessed >> 1, seqsPairs, alnRegVecPairs)
    }

    //testReads.foreach(r => samWriter.writeString((r.seq.sam)))
  }
*/

  // load reads from the FASTQ file (for testing use)
  def loadBatchFASTQSeqs(reader: BufferedReader, batchNum: Int): Array[FASTQSingleNode] = {
    
    var line = reader.readLine
    var i = 0
    var readIdx = 0    
    var seqs: Vector[FASTQSingleNode] = scala.collection.immutable.Vector.empty

    while(line != null && i < batchNum) {

      val lineFields = line.split(" ")
      var seq = new FASTQSingleNode
  
      if(lineFields.length == 1) {
        if(lineFields(0).charAt(0) == '@') seq.name = lineFields(0).substring(1).dropRight(2)
        else seq.name = lineFields(0).dropRight(2)
        seq.seq = reader.readLine
        seq.seqLen = seq.seq.size
        reader.readLine
        seq.qual = reader.readLine
        seq.comment = ""
        seqs = seqs :+ seq
        i += 1
      }
      else if(lineFields.length == 2) {
        if(lineFields(0).charAt(0) == '@') seq.name = lineFields(0).substring(1).dropRight(2)
        else seq.name = lineFields(0).dropRight(2)
        seq.comment = lineFields(1)
        seq.seq = reader.readLine
        seq.seqLen = seq.seq.size
        reader.readLine
        seq.qual = reader.readLine
        seqs = seqs :+ seq
        i += 1
      }
      else 
        println("Error: Input format not handled")

      if(i < batchNum)
        line = reader.readLine
    } 
    
    seqs.toArray
  }

  def loadBatchPairFASTQSeqs(reader1: BufferedReader, reader2: BufferedReader, batchNum: Int): Array[FASTQSingleNode] = {

    var line = reader1.readLine
    var i = 0
    var readIdx = 0    
    var seqs: Vector[FASTQSingleNode] = scala.collection.immutable.Vector.empty

    while(line != null && i < batchNum) {

      val lineFields = line.split(" ")
      var seq = new FASTQSingleNode
  
      if(lineFields.length == 1) {
        if(lineFields(0).charAt(0) == '@') seq.name = lineFields(0).substring(1).dropRight(2)
        else seq.name = lineFields(0).dropRight(2)
        seq.seq = reader1.readLine
        seq.seqLen = seq.seq.size
        reader1.readLine
        seq.qual = reader1.readLine
        seq.comment = ""
        seqs = seqs :+ seq
        i += 1
      }
      else if(lineFields.length == 2) {
        if(lineFields(0).charAt(0) == '@') seq.name = lineFields(0).substring(1).dropRight(2)
        else seq.name = lineFields(0).dropRight(2)
        seq.comment = lineFields(1)
        seq.seq = reader1.readLine
        seq.seqLen = seq.seq.size
        reader1.readLine
        seq.qual = reader1.readLine
        seqs = seqs :+ seq
        i += 1
      }
      else 
        println("Error: Input format not handled")

      line = reader2.readLine

      if(line == null) println("Error: the number of two FASTQ files are different")
      else {
        val lineFields = line.split(" ")
        var seq = new FASTQSingleNode

        if(lineFields.length == 1) {
          if(lineFields(0).charAt(0) == '@') seq.name = lineFields(0).substring(1).dropRight(2)
          else seq.name = lineFields(0).dropRight(2)
          seq.seq = reader2.readLine
          seq.seqLen = seq.seq.size
          reader2.readLine
          seq.qual = reader2.readLine
          seq.comment = ""
          seqs = seqs :+ seq
          i += 1
        }
        else if(lineFields.length == 2) {
          if(lineFields(0).charAt(0) == '@') seq.name = lineFields(0).substring(1).dropRight(2)
          else seq.name = lineFields(0).dropRight(2)
          seq.comment = lineFields(1)
          seq.seq = reader2.readLine
          seq.seqLen = seq.seq.size
          reader2.readLine
          seq.qual = reader2.readLine
          seqs = seqs :+ seq
          i += 1
        }
        else
          println("Error: Input format not handled")
      }

      if(i < batchNum)
        line = reader1.readLine
    } 
    
    seqs.toArray

  }

  class testRead {
    var seq: FASTQSingleNode = _
    var regs: MemAlnRegArrayType = _
  }

} 

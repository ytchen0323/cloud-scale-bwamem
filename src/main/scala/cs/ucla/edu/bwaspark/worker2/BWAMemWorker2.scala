package cs.ucla.edu.bwaspark.worker2

import scala.collection.immutable.Vector

import cs.ucla.edu.bwaspark.datatype._
import cs.ucla.edu.bwaspark.worker2.MemMarkPrimarySe._
import cs.ucla.edu.bwaspark.worker2.MemRegToADAMSAM._
import cs.ucla.edu.bwaspark.worker2.MemSamPe._
import cs.ucla.edu.bwaspark.sam.SAMHeader
import cs.ucla.edu.avro.fastq._

import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.adam.models.{SequenceDictionary, RecordGroup}

object BWAMemWorker2 {
  private val MEM_F_PE: Int = 0x2

  
  //pre-process: transform A/C/G/T to 0,1,2,3
  private def locusEncode(locus: Char): Byte = {
    //transforming from A/C/G/T to 0,1,2,3
    locus match {
      case 'A' => 0
      case 'a' => 0
      case 'C' => 1
      case 'c' => 1
      case 'G' => 2
      case 'g' => 2
      case 'T' => 3
      case 't' => 3
      case '-' => 5
      case _ => 4
    }
  }


  /**
    *  BWA-MEM Worker 2: used for single-end alignment
    *
    *  @param opt the input MemOptType object
    *  @param regs the alignment registers to be transformed
    *  @param bns the input BNTSeqType object
    *  @param pac the PAC array
    *  @param seq the read (NOTE: in the distributed version, we use FASTQRecord data structure.)
    *  @param numProcessed the number of reads that have been proceeded
    *  @param samHeader the SAM header required to output SAM strings
    *  @return the SAM format string of the given read
    */
  def singleEndBwaMemWorker2(opt: MemOptType, regs: Array[MemAlnRegType], bns: BNTSeqType, pac: Array[Byte], seq: FASTQRecord, numProcessed: Long, samHeader: SAMHeader): String = {
    var regsOut: Array[MemAlnRegType] = null
    if(regs != null)
      regsOut = memMarkPrimarySe(opt, regs, numProcessed)
    
    val seqStr = new String(seq.getSeq.array)
    val seqTrans: Array[Byte] = seqStr.toCharArray.map(ele => locusEncode(ele))

    memRegToSAMSe(opt, bns, pac, seq, seqTrans, regsOut, 0, null, samHeader)
  }


  /**
    *  BWA-MEM Worker 2: used for single-end alignment ADAM format output
    *
    *  @param opt the input MemOptType object
    *  @param regs the alignment registers to be transformed
    *  @param bns the input BNTSeqType object
    *  @param pac the PAC array
    *  @param seq the read (NOTE: in the distributed version, we use FASTQRecord data structure.)
    *  @param numProcessed the number of reads that have been proceeded
    *  @param samHeader the SAM header required to output SAM strings
    *  @param seqDict the sequences (chromosome) dictionary: used for ADAM format output
    *  @param readGroup the read group: used for ADAM format output
    *  @return the ADAM format object array of the given read
    */
  def singleEndBwaMemWorker2ADAMOut(opt: MemOptType, regs: Array[MemAlnRegType], bns: BNTSeqType, pac: Array[Byte], seq: FASTQRecord, 
                                    numProcessed: Long, samHeader: SAMHeader, seqDict: SequenceDictionary, readGroup: RecordGroup): Array[AlignmentRecord] = {
    var regsOut: Array[MemAlnRegType] = null
    if(regs != null)
      regsOut = memMarkPrimarySe(opt, regs, numProcessed)
    
    val seqStr = new String(seq.getSeq.array)
    val seqTrans: Array[Byte] = seqStr.toCharArray.map(ele => locusEncode(ele))

    memRegToADAMSe(opt, bns, pac, seq, seqTrans, regsOut, 0, null, samHeader, seqDict, readGroup).toArray
  }

  /**
    *  BWA-MEM Worker 2: used for pair-end alignment (No batched processing, No JNI with native libraries.)
    *
    *  @param opt the input MemOptType object
    *  @param bns the input BNSSeqType object
    *  @param pac the PAC array
    *  @param numProcessed the number of reads that have been proceeded
    *  @param pes the pair-end statistics array
    *  @param pairEndRead the PairEndReadType object with both the read and the alignments information
    *  @param samHeader the SAM header required to output SAM strings
    */
  def pairEndBwaMemWorker2(opt: MemOptType, bns: BNTSeqType, pac: Array[Byte], numProcessed: Long, pes: Array[MemPeStat], pairEndRead: PairEndReadType, samHeader: SAMHeader) {
    var alnRegVec: Array[Array[MemAlnRegType]] = new Array[Array[MemAlnRegType]](2)
    var seqs: PairEndFASTQRecord = new PairEndFASTQRecord
    seqs.seq0 = pairEndRead.seq0
    seqs.seq1 = pairEndRead.seq1
    alnRegVec(0) = pairEndRead.regs0
    alnRegVec(1) = pairEndRead.regs1
    memSamPe(opt, bns, pac, pes, numProcessed, seqs, alnRegVec, samHeader)
  }


  /**
    *  BWA-MEM Worker 2: used for pair-end alignment (batched processing + JNI with native libraries)
    *
    *  @param opt the input MemOptType object
    *  @param bns the input BNSSeqType object
    *  @param pac the PAC array
    *  @param numProcessed the number of reads that have been proceeded
    *  @param pes the pair-end statistics array
    *  @param pairEndReadArray the PairEndReadType object array. Each element has both the read and the alignments information
    *  @param subBatchSize the batch size of the number of reads to be sent to JNI library for native execution
    *  @param isPSWJNI the Boolean flag to mark whether the JNI native library is going to be used
    *  @param jniLibPath the JNI library path
    *  @param samHeader the SAM header required to output SAM strings
    */
  def pairEndBwaMemWorker2PSWBatched(opt: MemOptType, bns: BNTSeqType, pac: Array[Byte], numProcessed: Long, pes: Array[MemPeStat], 
                                     pairEndReadArray: Array[PairEndReadType], subBatchSize: Int, isPSWJNI: Boolean, jniLibPath: String, samHeader: SAMHeader) {
    var alnRegVecPairs: Array[Array[Array[MemAlnRegType]]] = new Array[Array[Array[MemAlnRegType]]](subBatchSize)
    var seqsPairs: Array[PairEndFASTQRecord] = new Array[PairEndFASTQRecord](subBatchSize)

    var i = 0
    while(i < subBatchSize) {
      alnRegVecPairs(i) = new Array[Array[MemAlnRegType]](2)
      seqsPairs(i) = new PairEndFASTQRecord
      seqsPairs(i).seq0 = pairEndReadArray(i).seq0
      seqsPairs(i).seq1 = pairEndReadArray(i).seq1
      alnRegVecPairs(i)(0) = pairEndReadArray(i).regs0
      alnRegVecPairs(i)(1) = pairEndReadArray(i).regs1
      i += 1
    }

    if(isPSWJNI) {
      System.load(jniLibPath)
      memSamPeGroupJNI(opt, bns, pac, pes, subBatchSize, numProcessed, seqsPairs, alnRegVecPairs, false, null, samHeader)
    }
    else
      memSamPeGroup(opt, bns, pac, pes, subBatchSize, numProcessed, seqsPairs, alnRegVecPairs, false, null, samHeader)
  }

  
  /**
    *  BWA-MEM Worker 2: used for pair-end alignment (batched processing + JNI with native libraries)
    *  In addition, the SAM string array with be returned to the driver node.
    *
    *  @param opt the input MemOptType object
    *  @param bns the input BNSSeqType object
    *  @param pac the PAC array
    *  @param numProcessed the number of reads that have been proceeded
    *  @param pes the pair-end statistics array
    *  @param pairEndReadArray the PairEndReadType object array. Each element has both the read and the alignments information
    *  @param subBatchSize the batch size of the number of reads to be sent to JNI library for native execution
    *  @param isPSWJNI the Boolean flag to mark whether the JNI native library is going to be used
    *  @param jniLibPath the JNI library path
    *  @param samHeader the SAM header required to output SAM strings
    */
  def pairEndBwaMemWorker2PSWBatchedSAMRet(opt: MemOptType, bns: BNTSeqType, pac: Array[Byte], numProcessed: Long, pes: Array[MemPeStat], 
                                           pairEndReadArray: Array[PairEndReadType], subBatchSize: Int, isPSWJNI: Boolean, jniLibPath: String, samHeader: SAMHeader): Array[Array[String]] = {
    var alnRegVecPairs: Array[Array[Array[MemAlnRegType]]] = new Array[Array[Array[MemAlnRegType]]](subBatchSize)
    var seqsPairs: Array[PairEndFASTQRecord] = new Array[PairEndFASTQRecord](subBatchSize)
    var samStringArray: Array[Array[String]] = new Array[Array[String]](subBatchSize) // return SAM string

    var i = 0
    while(i < subBatchSize) {
      alnRegVecPairs(i) = new Array[Array[MemAlnRegType]](2)
      samStringArray(i) = new Array[String](2)
      seqsPairs(i) = new PairEndFASTQRecord
      seqsPairs(i).seq0 = pairEndReadArray(i).seq0
      seqsPairs(i).seq1 = pairEndReadArray(i).seq1
      alnRegVecPairs(i)(0) = pairEndReadArray(i).regs0
      alnRegVecPairs(i)(1) = pairEndReadArray(i).regs1
      samStringArray(i)(0) = new String
      samStringArray(i)(1) = new String
      i += 1
    }

    if(isPSWJNI) {
      System.load(jniLibPath)
      memSamPeGroupJNI(opt, bns, pac, pes, subBatchSize, numProcessed, seqsPairs, alnRegVecPairs, true, samStringArray, samHeader)
    }
    else
      memSamPeGroup(opt, bns, pac, pes, subBatchSize, numProcessed, seqsPairs, alnRegVecPairs, true, samStringArray, samHeader)

    samStringArray
  }


  /**
    *  BWA-MEM Worker 2: used for pair-end alignment (batched processing + JNI with native libraries)
    *  In addition, the ADAM format output will be write back to the distributed file system
    *
    *  @param opt the input MemOptType object
    *  @param bns the input BNSSeqType object
    *  @param pac the PAC array
    *  @param numProcessed the number of reads that have been proceeded
    *  @param pes the pair-end statistics array
    *  @param pairEndReadArray the PairEndReadType object array. Each element has both the read and the alignments information
    *  @param subBatchSize the batch size of the number of reads to be sent to JNI library for native execution
    *  @param isPSWJNI the Boolean flag to mark whether the JNI native library is going to be used
    *  @param jniLibPath the JNI library path
    *  @param samHeader the SAM header required to output SAM strings
    *  @param seqDict the sequences (chromosome) dictionary: used for ADAM format output
    *  @param readGroup the read group: used for ADAM format output
    *  @return the ADAM format object array of the given read
    */
  def pairEndBwaMemWorker2PSWBatchedADAMRet(opt: MemOptType, bns: BNTSeqType, pac: Array[Byte], numProcessed: Long, pes: Array[MemPeStat], pairEndReadArray: Array[PairEndReadType], 
                                            subBatchSize: Int, isPSWJNI: Boolean, jniLibPath: String, samHeader: SAMHeader, seqDict: SequenceDictionary, readGroup: RecordGroup): Array[AlignmentRecord] = {
    var alnRegVecPairs: Array[Array[Array[MemAlnRegType]]] = new Array[Array[Array[MemAlnRegType]]](subBatchSize)
    var seqsPairs: Array[PairEndFASTQRecord] = new Array[PairEndFASTQRecord](subBatchSize)

    var i = 0
    while(i < subBatchSize) {
      alnRegVecPairs(i) = new Array[Array[MemAlnRegType]](2)
      seqsPairs(i) = new PairEndFASTQRecord
      seqsPairs(i).seq0 = pairEndReadArray(i).seq0
      seqsPairs(i).seq1 = pairEndReadArray(i).seq1
      alnRegVecPairs(i)(0) = pairEndReadArray(i).regs0
      alnRegVecPairs(i)(1) = pairEndReadArray(i).regs1
      i += 1
    }

    if(isPSWJNI) {
      System.load(jniLibPath)
      memADAMPeGroupJNI(opt, bns, pac, pes, subBatchSize, numProcessed, seqsPairs, alnRegVecPairs, samHeader, seqDict, readGroup)
    }
    else
      memADAMPeGroup(opt, bns, pac, pes, subBatchSize, numProcessed, seqsPairs, alnRegVecPairs, samHeader, seqDict, readGroup)
  }
}


package cs.ucla.edu.bwaspark.profiling

import cs.ucla.edu.bwaspark.datatype._
import scala.collection.mutable.MutableList
import java.util.TreeSet
import java.util.Comparator
import cs.ucla.edu.bwaspark.worker1.MemChain._
import cs.ucla.edu.bwaspark.worker1.MemChainFilter._
//import cs.ucla.edu.bwaspark.worker1.MemChainToAlignBatched._
import cs.ucla.edu.bwaspark.worker1.MemSortAndDedup._
import cs.ucla.edu.avro.fastq._
import cs.ucla.edu.bwaspark.debug.DebugFlag._

import cs.ucla.edu.bwaspark.profiling.MemChainToAlignBatchedProfile._

//this standalone object defines the main job of BWA MEM:
//1)for each read, generate all the possible seed chains
//2)using SW algorithm to extend each chain to all possible aligns
object BWAMemWorker1BatchedProfile {
  
  /**
    *  Perform BWAMEM worker1 function for single-end alignment
    *  Alignment is processed in a batched way
    *
    *  @param opt the MemOptType object, BWAMEM options
    *  @param bwt BWT and Suffix Array
    *  @param bns .ann, .amb files
    *  @param pac .pac file (PAC array: uint8_t)
    *  @param pes pes array for worker2
    *  @param seqArray a batch of reads
    *  @param numOfReads #reads in the batch
    *  
    *  Return: a batch of reads with alignments 
    */
  def bwaMemWorker1BatchedProfile
                          (opt: MemOptType, //BWA MEM options
                           bwt: BWTType, //BWT and Suffix Array
                           bns: BNTSeqType, //.ann, .amb files
                           pac: Array[Byte], //.pac file uint8_t
                           pes: Array[MemPeStat], //pes array
                           seqArray: Array[FASTQRecord], //the batched reads
                           numOfReads: Int, //the number of the batched reads
			   runOnFPGA: Boolean, //if run on FPGA
			   threshold: Int, //the batch threshold to run on FPGA
                           profileData: SWBatchTimeBreakdown
                           ): Array[ReadType] = { //all possible alignments for all the reads  

    //pre-process: transform A/C/G/T to 0,1,2,3

    // *****    PROFILING     *****
    val startTime = System.currentTimeMillis

    def locusEncode(locus: Char): Byte = {
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

    val readArray = new Array[Array[Byte]](numOfReads)
    val lenArray = new Array[Int](numOfReads)
    var i = 0
    while (i < numOfReads) {
      readArray(i) = (new String(seqArray(i).getSeq.array)).toCharArray.map(locus => locusEncode(locus))
      lenArray(i) = seqArray(i).getSeqLength.toInt
      i = i + 1
    }

    //first & second step: chaining and filtering
    val chainsFilteredArray = new Array[Array[MemChainType]](numOfReads)
    i = 0;
    while (i < numOfReads) {
      chainsFilteredArray(i) = memChainFilter(opt, generateChains(opt, bwt, bns.l_pac, lenArray(i), readArray(i))) 
      i = i+1;
    }

    // *****   PROFILING    *******
    val generatedChainEndTime = System.currentTimeMillis
    profileData.generatedChainTime = generatedChainEndTime - startTime

    val readRetArray = new Array[ReadType](numOfReads)
    i = 0;
    while (i < numOfReads) {
      readRetArray(i) = new ReadType
      readRetArray(i).seq = seqArray(i)
      i = i+1
    }

    val preResultsOfSW = new Array[Array[SWPreResultType]](numOfReads)
    val numOfSeedsArray = new Array[Int](numOfReads)
    val regArrays = new Array[MemAlnRegArrayType](numOfReads)
    i = 0;
    while (i < numOfReads) {
      if (chainsFilteredArray(i) == null) {
        preResultsOfSW(i) = null
        numOfSeedsArray(i) = 0
        regArrays(i) = null
      }
      else {
        preResultsOfSW(i) = new Array[SWPreResultType](chainsFilteredArray(i).length)
        var j = 0;
        while (j < chainsFilteredArray(i).length) {
          preResultsOfSW(i)(j)= calPreResultsOfSW(opt, bns.l_pac, pac, lenArray(i), readArray(i), chainsFilteredArray(i)(j))
    	    j = j+1
    	  }
        numOfSeedsArray(i) = 0
        chainsFilteredArray(i).foreach(chain => {
          numOfSeedsArray(i) += chain.seeds.length
          } )
        if (debugLevel == 1) println("Finished the calculation of pre-results of Smith-Waterman")
        if (debugLevel == 1) println("The number of reads in this pack is: " + numOfReads)
        regArrays(i) = new MemAlnRegArrayType
        regArrays(i).maxLength = numOfSeedsArray(i)
        regArrays(i).regs = new Array[MemAlnRegType](numOfSeedsArray(i))
      }
      i = i+1;
    }
    if (debugLevel == 1) println("Finished the pre-processing part")

    // *****   PROFILING    *******                
    val filterChainEndTime = System.currentTimeMillis
    profileData.filterChainTime = filterChainEndTime - generatedChainEndTime

    memChainToAlnBatchedProfile(opt, bns.l_pac, pac, lenArray, readArray, numOfReads, preResultsOfSW, chainsFilteredArray, regArrays, runOnFPGA, threshold, profileData)

    // *****   PROFILING    *******
    val chainToAlnEndTime = System.currentTimeMillis
    profileData.chainToAlnTime = chainToAlnEndTime - filterChainEndTime    

    if (debugLevel == 1) println("Finished the batched-processing part")
    regArrays.foreach(ele => {if (ele != null) ele.regs = ele.regs.filter(r => (r != null))})
    regArrays.foreach(ele => {if (ele != null) ele.maxLength = ele.regs.length})
    i = 0;
    while (i < numOfReads) {
      if (regArrays(i) == null) readRetArray(i).regs = null
      else readRetArray(i).regs = memSortAndDedup(regArrays(i), opt.maskLevelRedun).regs
      i = i+1
    }

    // *****   PROFILING    *******
    val sortAndDedupEndTime = System.currentTimeMillis
    profileData.sortAndDedupTime = sortAndDedupEndTime - chainToAlnEndTime

    readRetArray
  }

  /**
    *  Perform BWAMEM worker1 function for pair-end alignment
    *
    *  @param opt the MemOptType object, BWAMEM options
    *  @param bwt BWT and Suffix Array
    *  @param bns .ann, .amb files
    *  @param pac .pac file (PAC array: uint8_t)
    *  @param pes pes array for worker2
    *  @param pairSeqs a read with both ends
    *  
    *  Return: a read with alignments on both ends
    */
  def pairEndBwaMemWorker1BatchedProfile
                          (opt: MemOptType, //BWA MEM options
                           bwt: BWTType, //BWT and Suffix Array
                           bns: BNTSeqType, //.ann, .amb files
                           pac: Array[Byte], //.pac file uint8_t
                           pes: Array[MemPeStat], //pes array
			   seqArray0: Array[FASTQRecord], //the first batch
			   seqArray1: Array[FASTQRecord], //the second batch
			   numOfReads: Int, //the number of reads in each batch
			   runOnFPGA: Boolean, //if run on FPGA
			   threshold: Int, //the batch threshold to run on FPGA
                           jniSWExtendLibPath: String = null // SWExtend Library Path
                          ): PairEndBatchedProfile = { //all possible alignment
    if(jniSWExtendLibPath != null && runOnFPGA)
      System.load(jniSWExtendLibPath)
        
    var pairEndBatchProflie = new PairEndBatchedProfile
    var swBatch0Profile = new SWBatchTimeBreakdown
    val readArray0 = bwaMemWorker1BatchedProfile(opt, bwt, bns, pac, pes, seqArray0, numOfReads, runOnFPGA, threshold, swBatch0Profile)
    var swBatch1Profile = new SWBatchTimeBreakdown
    val readArray1 = bwaMemWorker1BatchedProfile(opt, bwt, bns, pac, pes, seqArray1, numOfReads, runOnFPGA, threshold, swBatch1Profile)
    var pairEndReadArray = new Array[PairEndReadType](numOfReads)
    var i = 0
    while (i < numOfReads) {
      pairEndReadArray(i) = new PairEndReadType
      pairEndReadArray(i).seq0 = readArray0(i).seq
      pairEndReadArray(i).regs0 = readArray0(i).regs
      pairEndReadArray(i).seq1 = readArray1(i).seq
      pairEndReadArray(i).regs1 = readArray1(i).regs
      i = i + 1
    }

    var pairEndTimeBreakdown = new SWBatchTimeBreakdown
    pairEndTimeBreakdown.initSWBatchTime = swBatch0Profile.initSWBatchTime + swBatch1Profile.initSWBatchTime
    pairEndTimeBreakdown.SWBatchRuntime = swBatch0Profile.SWBatchRuntime + swBatch1Profile.SWBatchRuntime
    pairEndTimeBreakdown.SWBatchOnFPGA = swBatch0Profile.SWBatchOnFPGA + swBatch1Profile.SWBatchOnFPGA
    pairEndTimeBreakdown.postProcessSWBatchTime = swBatch0Profile.postProcessSWBatchTime + swBatch1Profile.postProcessSWBatchTime
    pairEndTimeBreakdown.FPGADataPreProcTime = swBatch0Profile.FPGADataPreProcTime + swBatch1Profile.FPGADataPreProcTime
    pairEndTimeBreakdown.FPGARoutineRuntime = swBatch0Profile.FPGARoutineRuntime + swBatch1Profile.FPGARoutineRuntime
    pairEndTimeBreakdown.FPGADataPostProcTime = swBatch0Profile.FPGADataPostProcTime + swBatch1Profile.FPGADataPostProcTime
    pairEndTimeBreakdown.FPGATaskNum = swBatch0Profile.FPGATaskNum + swBatch1Profile.FPGATaskNum
    pairEndTimeBreakdown.CPUTaskNum = swBatch0Profile.CPUTaskNum + swBatch1Profile.CPUTaskNum
    pairEndTimeBreakdown.generatedChainTime = swBatch0Profile.generatedChainTime + swBatch1Profile.generatedChainTime
    pairEndTimeBreakdown.filterChainTime = swBatch0Profile.filterChainTime + swBatch1Profile.filterChainTime
    pairEndTimeBreakdown.chainToAlnTime = swBatch0Profile.chainToAlnTime + swBatch1Profile.chainToAlnTime
    pairEndTimeBreakdown.sortAndDedupTime = swBatch0Profile.sortAndDedupTime + swBatch1Profile.sortAndDedupTime

    pairEndBatchProflie.pairEndReadArray = pairEndReadArray
    pairEndBatchProflie.swBatchTimeBreakdown = pairEndTimeBreakdown
    pairEndBatchProflie // return
  }
}

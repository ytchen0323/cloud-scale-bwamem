package cs.ucla.edu.bwaspark.worker1

import cs.ucla.edu.bwaspark.datatype._
import scala.collection.mutable.MutableList
import java.util.TreeSet
import java.util.Comparator
import cs.ucla.edu.bwaspark.worker1.MemChain._
import cs.ucla.edu.bwaspark.worker1.MemChainFilter._
import cs.ucla.edu.bwaspark.worker1.MemChainToAlignBatched._
import cs.ucla.edu.bwaspark.worker1.MemSortAndDedup._
import cs.ucla.edu.avro.fastq._
import cs.ucla.edu.bwaspark.debug.DebugFlag._

//this standalone object defines the main job of BWA MEM:
//1)for each read, generate all the possible seed chains
//2)using SW algorithm to extend each chain to all possible aligns
object BWAMemWorker1Batched {
  
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
  def bwaMemWorker1Batched(opt: MemOptType, //BWA MEM options
                           bwt: BWTType, //BWT and Suffix Array
                           bns: BNTSeqType, //.ann, .amb files
                           pac: Array[Byte], //.pac file uint8_t
                           pes: Array[MemPeStat], //pes array
                           seqArray: Array[FASTQRecord], //the batched reads
                           numOfReads: Int //the number of the batched reads
                           ): Array[ReadType] = { //all possible alignments for all the reads  

    //for paired alignment, to add
    //!!!to add!!!
    //for now, we only focus on single sequence alignment
    //if (!(opt.flag & MEM_F_PE)) {
    if (true) {

      //pre-process: transform A/C/G/T to 0,1,2,3

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
      for (i <- 0 until numOfReads) readArray(i) = (new String(seqArray(i).getSeq.array)).toCharArray.map(locus => locusEncode(locus))
      val lenArray = new Array[Int](numOfReads)
      for (i <- 0 until numOfReads) lenArray(i) = seqArray(i).getSeqLength.toInt

      //first & second step: chaining and filtering
      val chainsFilteredArray = new Array[Array[MemChainType]](numOfReads)
      var i = 0;
      while (i < numOfReads) {
        chainsFilteredArray(i) = memChainFilter(opt, generateChains(opt, bwt, bns.l_pac, lenArray(i), readArray(i))) 
        i = i+1;
      }

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


      memChainToAlnBatched(opt, bns.l_pac, pac, lenArray, readArray, numOfReads, preResultsOfSW, chainsFilteredArray, regArrays)
      if (debugLevel == 1) println("Finished the batched-processing part")
      regArrays.foreach(ele => {if (ele != null) ele.regs = ele.regs.filter(r => (r != null))})
      regArrays.foreach(ele => {if (ele != null) ele.maxLength = ele.regs.length})
      i = 0;
      while (i < numOfReads) {
        if (regArrays(i) == null) readRetArray(i).regs = null
        else readRetArray(i).regs = memSortAndDedup(regArrays(i), opt.maskLevelRedun).regs
        i = i+1
      }
      readRetArray
    }
    else {
      assert (false)
      null
    }
  }
}
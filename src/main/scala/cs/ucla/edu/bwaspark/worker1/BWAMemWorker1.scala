package cs.ucla.edu.bwaspark.worker1

import cs.ucla.edu.bwaspark.datatype._
import scala.collection.mutable.MutableList
import java.util.TreeSet
import java.util.Comparator
import cs.ucla.edu.bwaspark.worker1.MemChain._
import cs.ucla.edu.bwaspark.worker1.MemChainFilter._
import cs.ucla.edu.bwaspark.worker1.MemChainToAlign._
import cs.ucla.edu.bwaspark.worker1.MemSortAndDedup._
import cs.ucla.edu.avro.fastq._

//this standalone object defines the main job of BWA MEM:
//1)for each read, generate all the possible seed chains
//2)using SW algorithm to extend each chain to all possible aligns
object BWAMemWorker1 {
  
  /**
    *  Perform BWAMEM worker1 function for single-end alignment
    *
    *  @param opt the MemOptType object, BWAMEM options
    *  @param bwt BWT and Suffix Array
    *  @param bns .ann, .amb files
    *  @param pac .pac file (PAC array: uint8_t)
    *  @param pes pes array for worker2
    *  @param seq a read
    *  
    *  Return: a read with alignments 
    */
  def bwaMemWorker1(opt: MemOptType, //BWA MEM options
                    bwt: BWTType, //BWT and Suffix Array
                    bns: BNTSeqType, //.ann, .amb files
                    pac: Array[Byte], //.pac file uint8_t
                    pes: Array[MemPeStat], //pes array
                    seq: FASTQRecord //a read
                    ): ReadType = { //all possible alignment  

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

    val seqStr = new String(seq.getSeq.array)
    val read: Array[Byte] = seqStr.toCharArray.map(ele => locusEncode(ele))

    //first step: generate all possible MEM chains for this read
    val chains = generateChains(opt, bwt, bns.l_pac, seq.getSeqLength, read)

    //second step: filter chains
    val chainsFiltered = memChainFilter(opt, chains)

    val readRet = new ReadType
    readRet.seq = seq

    if (chainsFiltered == null) {
      readRet.regs = null
    }
    else {
      // build the references of the seeds in each chain
      var totalSeedNum = 0
      chainsFiltered.foreach(chain => {
        totalSeedNum += chain.seeds.length
        } )

      //third step: for each chain, from chain to aligns
      var regArray = new MemAlnRegArrayType
      regArray.maxLength = totalSeedNum
      regArray.regs = new Array[MemAlnRegType](totalSeedNum)

      for (i <- 0 until chainsFiltered.length) {
        memChainToAln(opt, bns.l_pac, pac, seq.getSeqLength, read, chainsFiltered(i), regArray)
      }

      regArray.regs = regArray.regs.filter(r => (r != null))
      regArray.maxLength = regArray.regs.length
      assert(regArray.curLength == regArray.maxLength, "[Error] After filtering array elements")

      //last step: sorting and deduplication
      regArray = memSortAndDedup(regArray, opt.maskLevelRedun)
      readRet.regs = regArray.regs
    }

    readRet
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
  def pairEndBwaMemWorker1(opt: MemOptType, //BWA MEM options
                           bwt: BWTType, //BWT and Suffix Array
                           bns: BNTSeqType, //.ann, .amb files
                           pac: Array[Byte], //.pac file uint8_t
                           pes: Array[MemPeStat], //pes array
                           pairSeqs: PairEndFASTQRecord //a read
                          ): PairEndReadType = { //all possible alignment  
 
    val read0 = bwaMemWorker1(opt, bwt, bns, pac, pes, pairSeqs.seq0)
    val read1 = bwaMemWorker1(opt, bwt, bns, pac, pes, pairSeqs.seq1)
    var pairEndRead = new PairEndReadType
    pairEndRead.seq0 = read0.seq
    pairEndRead.regs0 = read0.regs
    pairEndRead.seq1 = read1.seq
    pairEndRead.regs1 = read1.regs

    pairEndRead // return
  }
}

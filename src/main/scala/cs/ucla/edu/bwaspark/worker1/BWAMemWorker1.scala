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
  
  //the function which do the main task
  def bwaMemWorker1(opt: MemOptType, //BWA MEM options
                    bwt: BWTType, //BWT and Suffix Array
                    bns: BNTSeqType, //.ann, .amb files
                    pac: Array[Byte], //.pac file uint8_t
                    pes: Array[MemPeStat], //pes array
                    seq: FASTQRecord //a read
                    ): ReadType = { //all possible alignment  

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
    else {
      assert (false)
      null
    }
  }
}

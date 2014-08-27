package cs.ucla.edu.bwaspark.worker2

import scala.collection.mutable.MutableList

import cs.ucla.edu.bwaspark.datatype._
import cs.ucla.edu.bwaspark.worker2.MemMarkPrimarySe._
import cs.ucla.edu.bwaspark.worker2.MemRegToADAMSAM._

object BWAMemWorker2 {
  /**
    *  Main function of BWA-mem worker2
    *
    *  @param opt the input MemOptType object
    *  @param regs the alignment registers to be transformed
    *  @param bns the input BNSSeqType object
    *  @param pac the PAC array
    *  @param seq the read (NOTE: currently we use Array[Byte] first. may need to be changed!!!)
    */
  def bwaMemWorker2(opt: MemOptType, regs: MutableList[MemAlnRegType], bns: BNTSeqType, pac: Array[Byte], seq: String, numProcessed: Long) {
    val regsOut = memMarkPrimarySe(opt, regs, numProcessed)
    
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

    memRegToSAMSe(opt, bns, pac, seq.toCharArray.map(ele => locusEncode(ele)), regsOut, 0, null)
  }

}


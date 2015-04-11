package cs.ucla.edu.bwaspark.worker2

import org.apache.commons.math3.special.Erf.erfc

import scala.math.sqrt
import scala.math.log
import scala.math.abs
import scala.collection.immutable.Vector

import cs.ucla.edu.bwaspark.datatype._
import cs.ucla.edu.bwaspark.worker1.MemSortAndDedup.memSortAndDedup
import cs.ucla.edu.bwaspark.util.BNTSeqUtil.bnsGetSeq
import cs.ucla.edu.bwaspark.util.SWUtil.SWAlign2
import cs.ucla.edu.bwaspark.worker2.MemMarkPrimarySe.{hash64, memMarkPrimarySe}
import cs.ucla.edu.bwaspark.worker2.MemRegToADAMSAM._
import cs.ucla.edu.bwaspark.jni.{MateSWJNI, MateSWType, SeqSWType, RefSWType}
import cs.ucla.edu.bwaspark.sam.SAMHeader
import cs.ucla.edu.avro.fastq._

import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.formats.avro.Contig
import org.bdgenomics.adam.models.{SequenceRecord, SequenceDictionary, RecordGroup}

import htsjdk.samtools.SAMRecord

// testing use
import java.io.FileReader
import java.io.BufferedReader
import java.math.BigInteger

object MemSamPe {
  private val MIN_RATIO = 0.8
  private val MIN_DIR_CNT = 10
  private val MIN_DIR_RATIO = 0.05
  private val OUTLIER_BOUND = 2.0
  private val MAPPING_BOUND = 3.0
  private val MAX_STDDEV = 4.0
  private val KSW_XBYTE = 0x10000
  private val KSW_XSTOP = 0x20000
  private val KSW_XSUBO = 0x40000
  private val KSW_XSTART = 0x80000
  private val M_SQRT1_2 = 7.0710678118654752440E-1  
  private val MEM_F_NO_RESCUE = 0x20
  private val MEM_F_NOPAIRING = 0x4

  private val NO_OUT_FILE = 0
  private val SAM_OUT_FILE = 1
  private val ADAM_OUT = 2

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
    *  Calculate the sub-score
    *
    *  @param opt the input MemOptType object
    *  @param regs the alignment array  
    *  @return the sub score
    */
  private def calSub(opt: MemOptType, regs: Array[MemAlnRegType]) : Int = {
    var j = 1
    var isBreak = false
    while(j < regs.length && !isBreak) { // choose unique alignment
      var bMax = regs(0).qBeg
      var eMin = regs(0).qEnd

      if(regs(j).qBeg > regs(0).qBeg) bMax = regs(j).qBeg
      if(regs(j).qEnd < regs(0).qEnd) eMin = regs(j).qEnd
      if(eMin > bMax) { // have overlap
        var minL = regs(0).qEnd - regs(0).qBeg
        if(regs(j).qEnd - regs(j).qBeg < minL) minL = regs(j).qEnd - regs(j).qBeg
        if(eMin - bMax >= minL * opt.maskLevel) { 
          isBreak = true
          j -= 1
        }
      }

      j += 1    
    }

    if(j < regs.length) regs(j).score
    else opt.minSeedLen * opt.a
  }


  /******************************************************************************************************************/
  /*************     MemSamPe.scala: Initial version (no batched processing, pure Java)     *************************/
  /******************************************************************************************************************/


  /**
    *  Calculate the pair-end statistics
    *
    *  @param opt the input MemOptType object
    *  @param pacLen the length of the PAC array
    *  @param n the number of seqs of this batch (= # of reads x 2)
    *  @param regArray the alignment array
    *  @param pes the pair-end statistics (output)
    */
  def memPeStat(opt: MemOptType, pacLen: Long, n: Int, regArray: Array[MemAlnRegArrayType], pes: Array[MemPeStat]) {
    var iSize: Array[Vector[Int]] = new Array[Vector[Int]](4)
    var j = 0
    while(j < 4) {
      iSize(j) = scala.collection.immutable.Vector.empty
      j += 1
    }

    var i = 0
    while(i < (n>>>1)) {
      var r: Array[MemAlnRegArrayType] = new Array[MemAlnRegArrayType](2)
      var dir: Int = 0
      var is: Long = 0
      r(0) = regArray(i<<1|0)
      r(1) = regArray(i<<1|1)

      if(r(0) != null && r(1) != null) {
        if(calSub(opt, r(0).regs) <= MIN_RATIO * r(0).regs(0).score) {
          if(calSub(opt, r(1).regs) <= MIN_RATIO * r(1).regs(0).score) {
            // Inline mem_infer_dir
            var r1: Boolean = false
            var r2: Boolean = false
            if(r(0).regs(0).rBeg >= pacLen) r1 = true
            if(r(1).regs(0).rBeg >= pacLen) r2 = true

            var rBegLarger = r(1).regs(0).rBeg
            // rBegLarger is the coordinate of read 2 on the read 1 strand
            if(r1 != r2) rBegLarger = (pacLen << 1) - 1 - r(1).regs(0).rBeg 
            var dist: Int = (r(0).regs(0).rBeg - rBegLarger).toInt
            if(rBegLarger > r(0).regs(0).rBeg) dist = (rBegLarger - r(0).regs(0).rBeg).toInt
            
            var cond1 = 1
            if(r1 == r2) cond1 = 0
            var cond2 = 3
            if(rBegLarger > r(0).regs(0).rBeg) cond2 = 0
            
            dir = cond1 ^ cond2
            if(dist > 0 && dist <= opt.maxIns) iSize(dir) = iSize(dir) :+ dist
          }
        }
      }

      i += 1
    }
   
    var d = 0
    while(d < 4) {
      val qInit: Vector[Int] = iSize(d)
      if(qInit.size < MIN_DIR_CNT) {
        if(d == 0)
          println("skip orientation FF as there are not enough pairs")
        else if(d == 1)
          println("skip orientation FR as there are not enough pairs")
        else if(d == 2)
          println("skip orientation RF as there are not enough pairs")
        else if(d == 3)
          println("skip orientation RR as there are not enough pairs")
        pes(d).failed = 1
        d += 1
      } 
      else {
        println("analyzing insert size distribution for orientation")
        val q = qInit.sortWith(_.compareTo(_) < 0) // ks_introsort_64
        var p25: Int = q((0.25 * iSize(d).size + 0.499).toInt)
        var p50: Int = q((0.50 * iSize(d).size + 0.499).toInt)
        var p75: Int = q((0.75 * iSize(d).size + 0.499).toInt)
        pes(d).low = (p25 - OUTLIER_BOUND * (p75 - p25) + 0.499).toInt
        if(pes(d).low < 1) pes(d).low = 1
        pes(d).high = (p75 + OUTLIER_BOUND * (p75 - p25) + 0.499).toInt
        println("(25, 50, 75) percentile: (" + p25 + ", " + p50 + ", " + p75 + ")")
        println("low and high boundaries for computing mean and std.dev: (" + pes(d).low + ", " + pes(d).high + ")")

        i = 0
        var x = 0
        pes(d).avg = 0
        while(i < q.size) {
          if(q(i) >= pes(d).low && q(i) <= pes(d).high) {
            pes(d).avg += q(i)
            x += 1
          }
          i += 1
        }
        pes(d).avg /= x
        
        i = 0
        pes(d).std = 0
        while(i < q.size) {
          if(q(i) >= pes(d).low && q(i) <= pes(d).high)
            pes(d).std += (q(i) - pes(d).avg) * (q(i) - pes(d).avg)

          i += 1
        }
        pes(d).std = sqrt(pes(d).std / x)
        println("mean and std.dev: (" + pes(d).avg + ", " + pes(d).std + ")")

        pes(d).low = (p25 - MAPPING_BOUND * (p75 - p25) + .499).toInt
        pes(d).high = (p75 + MAPPING_BOUND * (p75 - p25) + .499).toInt
        if(pes(d).low > pes(d).avg - MAX_STDDEV * pes(d).std) pes(d).low = (pes(d).avg - MAX_STDDEV * pes(d).std + .499).toInt
        if(pes(d).high < pes(d).avg - MAX_STDDEV * pes(d).std) pes(d).high = (pes(d).avg - MAX_STDDEV * pes(d).std + .499).toInt
        if(pes(d).low < 1) pes(d).low = 1
        println("low and high boundaries for proper pairs: (" + pes(d).low + ", " + pes(d).high + ")")

        d += 1
      }
    } 

    d = 0
    var max = 0
    while(d < 4) {
      if(max < iSize(d).size) max = iSize(d).size
      d += 1
    }

    d = 0
    while(d < 4) {
      if(pes(d).failed == 0 && iSize(d).size < max * MIN_DIR_RATIO) {
        pes(d).failed = 1
        if(d == 0)
          println("skip orientation FF")
        else if(d == 1)
          println("skip orientation FR")
        else if(d == 2)
          println("skip orientation RF")
        else if(d == 3)
          println("skip orientation RR")
      }     

      d += 1
    }
  }

 
  /**
    *  Mate Smith-Waterman algorithm
    *  Used for single read without batched processing.
    *
    *  @param opt the input MemOptType object
    *  @param pacLen the length of the PAC array
    *  @param pac the PAC array
    *  @param pes the pair-end statistics (output)
    *  @param reg the alignment
    *  @param mateSeqLen the length of the mate sequence
    *  @param mateRegs the alignments of the mate sequence
    *  @return (the number of added new alignments from mate-SW, alignment array)
    */
  private def memMateSw(opt: MemOptType, pacLen: Long, pac: Array[Byte], pes: Array[MemPeStat], reg: MemAlnRegType, 
                        mateSeqLen: Int, mateSeq: Array[Byte], mateRegs: Array[MemAlnRegType]): (Int, Array[MemAlnRegType]) = {
    var mateRegsUpdated: Vector[MemAlnRegType] = scala.collection.immutable.Vector.empty
    var skip: Array[Int] = new Array[Int](4)
    var n = 0
    var regArray = new MemAlnRegArrayType

    var r = 0
    while(r < 4) {
      if(pes(r).failed > 0) skip(r) = 1
      else skip(r) = 0
      r += 1     
    }

    var i = 0
    if(mateRegs != null) {
      while(i < mateRegs.size) { // check which orinentation has been found
        // Inline mem_infer_dir
        var r1: Boolean = false
        var r2: Boolean = false
        if(reg.rBeg >= pacLen) r1 = true
        if(mateRegs(i).rBeg >= pacLen) r2 = true

        var rBegLarger = mateRegs(i).rBeg
        // rBegLarger is the coordinate of read 2 on the read 1 strand
        if(r1 != r2) rBegLarger = (pacLen << 1) - 1 - mateRegs(i).rBeg
        var dist: Int = (reg.rBeg - rBegLarger).toInt
        if(rBegLarger > reg.rBeg) dist = (rBegLarger - reg.rBeg).toInt

        var cond1 = 1
        if(r1 == r2) cond1 = 0
        var cond2 = 3
        if(rBegLarger > reg.rBeg) cond2 = 0

        r = cond1 ^ cond2      
        if(dist >= pes(r).low && dist <= pes(r).high) skip(r) = 1     

        i += 1
      }
    }
 
    if(skip(0) + skip(1) + skip(2) + skip(3) == 4) (0, mateRegs)   // consistent pair exist; no need to perform SW
 
    // NOTE!!!: performance can be further optimized to avoid the copy
    if(mateRegs != null) {
      i = 0
      while(i < mateRegs.size) {
        mateRegsUpdated = mateRegsUpdated :+ mateRegs(i)
        i += 1
      }
    }

    r = 0
    while(r < 4) {
      if(skip(r) == 0) {
        var seq: Array[Byte] = mateSeq
        var rBeg: Long = -1
        var rEnd: Long = -1
        var len: Long = 0

        var isRev = 0
        if((r >> 1) != (r & 1)) isRev = 1   // whether to reverse complement the mate

        var isLarger = 1
        if((r >> 1) > 0) isLarger = 0   // whether the mate has larger coordinate

        if(isRev > 0) {
          var rev: Array[Byte] = new Array[Byte](mateSeqLen)
          i = 0
          while(i < mateSeqLen) {
            if(mateSeq(i) < 4) rev(mateSeqLen - 1 - i) = (3 - mateSeq(i)).toByte
            else rev(mateSeqLen - 1 - i) = 4
            i += 1
          }
          seq = rev
        }
        
        if(isRev == 0) {
          if(isLarger > 0) rBeg = reg.rBeg + pes(r).low
          else rBeg = reg.rBeg - pes(r).high

          if(isLarger > 0) rEnd = reg.rBeg + pes(r).high + mateSeqLen // if on the same strand, end position should be larger to make room for the seq length
          else rEnd = reg.rBeg - pes(r).low + mateSeqLen
        }
        else {
          if(isLarger > 0) rBeg = reg.rBeg + pes(r).low - mateSeqLen // similarly on opposite strands
          else rBeg = reg.rBeg - pes(r).high - mateSeqLen

          if(isLarger > 0) rEnd = reg.rBeg + pes(r).high
          else rEnd = reg.rBeg - pes(r).low
        }

        if(rBeg < 0) rBeg = 0
        if(rEnd > (pacLen << 1)) rEnd = pacLen << 1

        val ref = bnsGetSeq(pacLen, pac, rBeg, rEnd) 
        if(ref._2 == rEnd - rBeg) { // no funny things happening  ( ref._2 -> len)
          var xtraTmp = 0
          if(mateSeqLen * opt.a < 250) xtraTmp = KSW_XBYTE
          val xtra = KSW_XSUBO | KSW_XSTART | xtraTmp | (opt.minSeedLen * opt.a)
          val aln = SWAlign2(mateSeqLen, seq, ref._2.toInt, ref._1, 5, opt, xtra) // ref._1 -> ref
          
          //println("aln score: " + aln.score + ", xtra: " + xtra)  
          var alnTmp = new MemAlnRegType
          if(aln.score >= opt.minSeedLen && aln.qBeg >= 0) { // something goes wrong if aln.qBeg < 0
            if(isRev > 0) {
              alnTmp.qBeg = mateSeqLen - (aln.qEnd + 1)
              alnTmp.qEnd = mateSeqLen - aln.qBeg
              alnTmp.rBeg = (pacLen << 1) - (rBeg + aln.tEnd + 1)
              alnTmp.rEnd = (pacLen << 1) - (rBeg + aln.tBeg)
            }
            else {
              alnTmp.qBeg = aln.qBeg
              alnTmp.qEnd = aln.qEnd + 1
              alnTmp.rBeg = rBeg + aln.tEnd + 1
              alnTmp.rEnd = rBeg + aln.tEnd + 1
            }

            alnTmp.score = aln.score
            alnTmp.csub = aln.scoreSecond
            alnTmp.secondary = -1

            if(alnTmp.rEnd - alnTmp.rBeg < alnTmp.qEnd - alnTmp.qBeg) alnTmp.seedCov = ((alnTmp.rEnd - alnTmp.rBeg) >>> 1).toInt
            else alnTmp.seedCov = (alnTmp.qEnd - alnTmp.qBeg) >>> 1
            
            // move b s.t. ma is sorted
            mateRegsUpdated = mateRegsUpdated :+ alnTmp

            // original sorting in c codes
            /*
            i = 0
            var isBreak = false
            while(i < mateRegs.size && isBreak) {
              if(mateRegs(i).score < alnTmp.score) isBreak = true
              else i += 1
            }
            val breakIdx = i

            i = mateRegs.size - 1
            while(i > breakIdx) {
              mateRegs(i) = mateRegs(i - 1)
              i -= 1
            }
            mateRegs(breakIdx) = alnTmp
            */
          }
          
          n += 1
        }

        if(n > 0) {
          // Sort here!
          mateRegsUpdated = mateRegsUpdated.sortBy(seq => (seq.score))
          regArray.regs = mateRegsUpdated.toArray
          regArray.curLength = mateRegsUpdated.size
          regArray.maxLength = mateRegsUpdated.size
          regArray = memSortAndDedup(regArray, opt.maskLevelRedun)
          
        }
      }

      r += 1
    }

    if(n > 0) (n, regArray.regs)
    else (n, mateRegs)
  }


  /**
    *  Data structure which keeps a pair of Long integer
    */
  class PairLong {
    var x: Long = 0
    var y: Long = 0
  } 


  /**
    *  Data structure which keeps a pair of Long integer
    */
  class QuadLong {
    var xMSB: Long = 0
    var xRest: Long = 0
    var x: Long = 0
    var y: Long = 0
  } 


  /**
    *  Pairing single-end hits
    *  Used for single read without batched processing.
    *
    *  @param opt the input MemOptType object
    *  @param pacLen the length of the PAC array
    *  @param pes the pair-end statistics (output)
    *  @param alnRegVec the alignments of both ends
    *  @param id the current read id
    *  @return (return value, subo value, n_sub, z[2])
    */
  private def memPair(opt: MemOptType, pacLen: Long, pes: Array[MemPeStat], alnRegVec: Array[Array[MemAlnRegType]], id: Long): (Int, Int, Int, Array[Int]) = {
    var keyVec: Vector[PairLong] = scala.collection.immutable.Vector.empty
    var keyUVec: Vector[PairLong] = scala.collection.immutable.Vector.empty
    var y: Array[Int] = new Array[Int](4)
    var r = 0
    var i = 0
    var z: Array[Int] = new Array[Int](2)

    z(0) = -1
    z(1) = -1

    while(r < 2) {
      i = 0
      while(i < alnRegVec(r).size) {
        var key: PairLong = new PairLong

        key.x = alnRegVec(r)(i).rBeg
        if(alnRegVec(r)(i).rBeg >= pacLen) key.x = (pacLen << 1) - 1 - alnRegVec(r)(i).rBeg   // forward position
        if(alnRegVec(r)(i).rBeg >= pacLen) key.y = (alnRegVec(r)(i).score.toLong << 32) | (i << 2) | 2 | r
        else key.y = (alnRegVec(r)(i).score.toLong << 32) | (i << 2) | 0 | r

        keyVec = keyVec :+ key
        i += 1
      }

      r += 1
    }

    val sortedKeyVec = keyVec.sortBy(key => (key.x, key.y))
    y(0) = -1 
    y(1) = -1
    y(2) = -1
    y(3) = -1

    i = 0
    while(i < sortedKeyVec.size) {
      r = 0
      while(r < 2) { // loop through direction
        var dir = ((r << 1) | (sortedKeyVec(i).y >>> 1 & 1)).toInt
        var which: Int = 0
        if(pes(dir).failed > 0) { // invalid orientation
          r += 1
        }
        else {
          which = ((r << 1) | ((sortedKeyVec(i).y & 1) ^ 1)).toInt
          if(y(which) < 0) { // no previous hits
            r += 1
          }
          else {
            var k = y(which)
            var isBreak = false
            while(k >= 0 && !isBreak) { // TODO: this is a O(n^2) solution in the worst case; remember to check if this loop takes a lot of time (I doubt)
              if((sortedKeyVec(k).y & 3) != which) 
                k -= 1
              else {
                var dist: Long = sortedKeyVec(i).x - sortedKeyVec(k).x
                if(dist > pes(dir).high) isBreak = true
                else if(dist < pes(dir).low) k -= 1
                else {
                  var ns: Double = (dist - pes(dir).avg) / pes(dir).std
                  var q: Int = ((sortedKeyVec(i).y >>> 32) + (sortedKeyVec(k).y >>> 32) + 0.721 * log(2.0 * erfc(abs(ns) * M_SQRT1_2)) * opt.a + 0.499).toInt
                  if(q < 0) q = 0
                  
                  var key: PairLong = new PairLong
                  key.y = k.toLong << 32 | i
                  key.x = q.toLong << 32 | ((hash64(key.y ^ id << 8) << 32) >>> 32) // modify from (hash64(key.y ^ id << 8) & 0xffffffff)
                  keyUVec = keyUVec :+ key
                  k -= 1
                }
              }
            }
            
            r += 1
          }
        }
      }      

      y((sortedKeyVec(i).y & 3).toInt) = i
      i += 1
    }

    var ret = 0
    var sub = 0
    var numSub = 0
    if(keyUVec.size > 0) { // found at least one proper pair
      var tmp: Int = opt.a + opt.b
      if(tmp < opt.oDel + opt.eDel) tmp = opt.oDel + opt.eDel
      if(tmp < opt.oIns + opt.eIns) tmp = opt.oIns + opt.eIns
      
      val sortedKeyUVec = keyUVec.sortBy(key => (key.x, key.y))
      var i = (sortedKeyUVec(sortedKeyUVec.size - 1).y >>> 32).toInt
      var k = (sortedKeyUVec(sortedKeyUVec.size - 1).y << 32 >>> 32).toInt
      z((sortedKeyVec(i).y & 1).toInt) = (sortedKeyVec(i).y << 32 >>> 34).toInt
      z((sortedKeyVec(k).y & 1).toInt) = (sortedKeyVec(k).y << 32 >>> 34).toInt
      ret = (sortedKeyUVec(sortedKeyUVec.size - 1).x >>> 32).toInt
      if(sortedKeyUVec.size > 1) sub = (sortedKeyUVec(sortedKeyUVec.size - 2).x >>> 32).toInt

      i = sortedKeyUVec.size - 2
      while(i >= 0) {
        if(sub - (sortedKeyUVec(i).x >>> 32).toInt <= tmp) numSub += 1
        i -= 1
      }
    } 
    else {
      ret = 0
      sub = 0
      numSub = 0
    }

    (ret, sub, numSub, z)
  }


  /**
    *  memSamPe: Pair-end alignments to SAM format 
    *  Used for single read without batched processing. (pure Java)
    *
    *  @param opt the input MemOptType object
    *  @param bns the BNTSeqType object
    *  @param pac the PAC array
    *  @param pes the pair-end statistics (output)
    *  @param id the current read id
    *  @param seqsIn the input pair-end read (from Parquet/Avro data format)
    *  @param alnRegVec the alignments of this read
    *  @param samHeader the SAM header required to output SAM strings
    *  @return the number of added new alignments from mate-SW
    */
  def memSamPe(opt: MemOptType, bns: BNTSeqType, pac: Array[Byte], pes: Array[MemPeStat], id: Long, 
               seqsIn: PairEndFASTQRecord, alnRegVec: Array[Array[MemAlnRegType]], samHeader: SAMHeader): Int = {
    var n: Int = 0
    var z: Array[Int] = new Array[Int](2)
    var subo: Int = 0
    var numSub: Int = 0
    var extraFlag: Int = 1
    var seqsTrans: Array[Array[Byte]] = new Array[Array[Byte]](2)
    var noPairingFlag: Boolean = false

    var seqs = new Array[FASTQRecord](2)
    seqs(0) = seqsIn.getSeq0
    seqs(1) = seqsIn.getSeq1

    val seqStr0 = new String(seqs(0).seq.array)
    seqsTrans(0) = seqStr0.toCharArray.map(ele => locusEncode(ele))
    val seqStr1 = new String(seqs(1).seq.array)
    seqsTrans(1) = seqStr1.toCharArray.map(ele => locusEncode(ele))

    if((opt.flag & MEM_F_NO_RESCUE) == 0) { // then perform SW for the best alignment
      var i = 0
      var alnRegTmpVec = new Array[Vector[MemAlnRegType]](2)
      alnRegTmpVec(0) = scala.collection.immutable.Vector.empty
      alnRegTmpVec(1) = scala.collection.immutable.Vector.empty

      while(i < 2) {
        if(alnRegVec(i) != null) {
          var j = 0
          while(j < alnRegVec(i).size) {
            if(alnRegVec(i)(j).score >= alnRegVec(i)(0).score - opt.penUnpaired)
              alnRegTmpVec(i) = alnRegTmpVec(i) :+ alnRegVec(i)(j)
            j += 1
          }
        }

        i += 1
      }

      i = 0
      while(i < 2) {
        var j = 0
        while(j < alnRegTmpVec(i).size && j < opt.maxMatesw) {
          var iBar = 0
          if(i == 0) iBar = 1
          val ret = memMateSw(opt, bns.l_pac, pac, pes, alnRegTmpVec(i)(j), seqs(iBar).seqLength, seqsTrans(iBar), alnRegVec(iBar)) 
          n += ret._1
          alnRegVec(iBar) = ret._2
          j += 1
        }

        i += 1
      }  
    } 
    
    alnRegVec(0) = memMarkPrimarySe(opt, alnRegVec(0), id<<1|0)
    alnRegVec(1) = memMarkPrimarySe(opt, alnRegVec(1), id<<1|1)

    if((opt.flag & MEM_F_NOPAIRING) == 0) {
      // pairing single-end hits
      var a0Size = 0
      var a1Size = 0
      if(alnRegVec(0) != null) a0Size = alnRegVec(0).size
      if(alnRegVec(1) != null) a1Size = alnRegVec(1).size

      if(alnRegVec(0) != null && alnRegVec(1) != null) {
        val retVal = memPair(opt, bns.l_pac, pes, alnRegVec, id)
        val ret = retVal._1
        subo = retVal._2
        numSub = retVal._3
        z = retVal._4

        if(ret > 0) {
          var scoreUn: Int = 0
          var isMulti: Array[Boolean] = new Array[Boolean](2)
          var qPe: Int = 0
          var qSe: Array[Int] = new Array[Int](2)

          var i = 0
          while(i < 2) {
            var j = 1
            var isBreak = false
            while(j < alnRegVec(i).size && !isBreak) {
              if(alnRegVec(i)(j).secondary < 0 && alnRegVec(i)(j).score >= opt.T) 
                isBreak = true
              else 
                j += 1
            }
              
            if(j < alnRegVec(i).size) isMulti(i) = true
            else isMulti(i) = false

            i += 1
          }

          if(!isMulti(0) && !isMulti(1)) {  
            // compute mapQ for the best SE hit
            scoreUn = alnRegVec(0)(0).score + alnRegVec(1)(0).score - opt.penUnpaired
            if(subo < scoreUn) subo = scoreUn
            // Inline raw_mapq(ret - subo, opt.a)
            // #define raw_mapq(diff, a) ((int)(6.02 * (diff) / (a) + .499))
            qPe = (6.02 * (ret - subo) / opt.a + 0.499).toInt
            
            if(numSub > 0) qPe -= (4.343 * log(numSub + 1) + .499).toInt
            if(qPe < 0) qPe = 0
            if(qPe > 60) qPe = 60

            // the following assumes no split hits
            if(ret > scoreUn) { // paired alignment is preferred
              var tmpRegs = new Array[MemAlnRegType](2)
              tmpRegs(0) = alnRegVec(0)(z(0))
              tmpRegs(1) = alnRegVec(1)(z(1))

              var i = 0
              while(i < 2) {
                if(tmpRegs(i).secondary >= 0) {
                  tmpRegs(i).sub = alnRegVec(i)(tmpRegs(i).secondary).score
                  tmpRegs(i).secondary = -1
                }

                qSe(i) = memApproxMapqSe(opt, tmpRegs(i))
                i += 1
              }

              if(qSe(0) < qPe) {
                if(qPe < qSe(0) + 40) qSe(0) = qPe
                else qSe(0) = qSe(0) + 40
              }

              if(qSe(1) < qPe) {
                if(qPe < qSe(1) + 40) qSe(1) = qPe
                else qSe(1) = qSe(1) + 40
              }
              
              extraFlag |= 2
                           
              // cap at the tandem repeat score
              // Inline raw_mapq(tmpRegs(0).score - tmpRegs(0).csub, opt.a)
              var tmp = (6.02 * (tmpRegs(0).score - tmpRegs(0).csub) / opt.a + 0.499).toInt
              if(qSe(0) > tmp) qSe(0) = tmp
              // Inline raw_mapq(tmpRegs(1).score - tmpRegs(1).csub, opt.a)
              tmp = (6.02 * (tmpRegs(1).score - tmpRegs(1).csub) / opt.a + 0.499).toInt
              if(qSe(1) > tmp) qSe(1) = tmp
            }
            else { // the unpaired alignment is preferred
              z(0) = 0
              z(1) = 0
              qSe(0) = memApproxMapqSe(opt, alnRegVec(0)(0))
              qSe(1) = memApproxMapqSe(opt, alnRegVec(1)(0))
            }
            
            // write SAM
            val aln0 = memRegToAln(opt, bns, pac, seqs(0).seqLength, seqsTrans(0), alnRegVec(0)(z(0))) 
            aln0.mapq = qSe(0).toShort
            aln0.flag |= 0x40 | extraFlag
            val aln1 = memRegToAln(opt, bns, pac, seqs(1).seqLength, seqsTrans(1), alnRegVec(1)(z(1)))
            aln1.mapq = qSe(1).toShort
            aln1.flag |= 0x80 | extraFlag

            var samStr0 = new SAMString
            var alnList0 = new Array[MemAlnType](1)
            alnList0(0) = aln0
            memAlnToSAM(bns, seqs(0), seqsTrans(0), alnList0, 0, aln1, samHeader, samStr0)
            // NOTE: temporarily comment out in Spark version
            //seqs(0).sam = samStr0.str.dropRight(samStr0.size - samStr0.idx).mkString
            var samStr1 = new SAMString
            var alnList1 = new Array[MemAlnType](1)
            alnList1(0) = aln1
            memAlnToSAM(bns, seqs(1), seqsTrans(1), alnList1, 0, aln0, samHeader, samStr1)
            // NOTE: temporarily comment out in Spark version
            //seqs(1).sam = samStr1.str.dropRight(samStr1.size - samStr1.idx).mkString

            // NOTE: temporarily comment out in Spark version
            //if(seqs(0).name != seqs(1).name) println("[Error] paired reads have different names: " + seqs(0).name + ", " + seqs(1).name)
            
          }
          else // else: goto no_pairing (TODO: in rare cases, the true hit may be long but with low score)
            noPairingFlag = true
        }
        else // else: goto no_pairing
          noPairingFlag = true
      }
      else // else: goto no_pairing 
        noPairingFlag = true 
    }
    else // else: goto no_pairing
      noPairingFlag = true

    if(!noPairingFlag) n
    else { // no_pairing: start from here
      var alnVec: Array[MemAlnType] = new Array[MemAlnType](2)

      var i = 0
      while(i < 2) {
        if(alnRegVec(i) != null && alnRegVec(i).size > 0 && alnRegVec(i)(0).score >= opt.T) alnVec(i) = memRegToAln(opt, bns, pac, seqs(i).seqLength, seqsTrans(i), alnRegVec(i)(0)) 
        else alnVec(i) = memRegToAln(opt, bns, pac, seqs(i).seqLength, seqsTrans(i), null) 

        i += 1
      }

      // if the top hits from the two ends constitute a proper pair, flag it.
      if((opt.flag & MEM_F_NOPAIRING) == 0 && alnVec(0).rid == alnVec(1).rid && alnVec(0).rid >= 0) {
        // Inline mem_infer_dir
        var r1: Boolean = false
        var r2: Boolean = false
        if(alnRegVec(0)(0).rBeg >= bns.l_pac) r1 = true
        if(alnRegVec(1)(0).rBeg >= bns.l_pac) r2 = true

        var rBegLarger = alnRegVec(1)(0).rBeg
        // rBegLarger is the coordinate of read 2 on the read 1 strand
        if(r1 != r2) rBegLarger = (bns.l_pac << 1) - 1 - alnRegVec(1)(0).rBeg
        var dist: Int = (alnRegVec(0)(0).rBeg - rBegLarger).toInt
        if(rBegLarger > alnRegVec(0)(0).rBeg) dist = (rBegLarger - alnRegVec(0)(0).rBeg).toInt

        var cond1 = 1
        if(r1 == r2) cond1 = 0
        var cond2 = 3
        if(rBegLarger > alnRegVec(0)(0).rBeg) cond2 = 0

        var d = cond1 ^ cond2
     
        if(pes(d).failed == 0 && dist >= pes(d).low && dist <= pes(d).high) extraFlag |= 2 
      }

      memRegToSAMSe(opt, bns, pac, seqs(0), seqsTrans(0), alnRegVec(0), 0x41 | extraFlag, alnVec(1), samHeader)
      memRegToSAMSe(opt, bns, pac, seqs(1), seqsTrans(1), alnRegVec(1), 0x81 | extraFlag, alnVec(0), samHeader)

      if(seqs(0).name != seqs(1).name) println("[Error] paired reads have different names: " + seqs(0).name + ", " + seqs(1).name)

      n   // return value
    }

  }

  /******************************************************************************************************************/
  /**********************     END of Initial version (no batched processing, pure Java)     *************************/
  /******************************************************************************************************************/


  /******************************************************************************************************************/
  /*************     MemSamPe.scala: batched version (batched processing, pure Java)     ****************************/
  /******************************************************************************************************************/


  /**
    *  Get the reference segments, including rBeg, rEnd, ref (reference segment), and len.
    *  Used before the batched processing.
    *
    *  @param pacLen the length of the PAC array
    *  @param pac the PAC array
    *  @param pes the pair-end statistics (output)
    *  @param reg the alignment
    *  @param mateSeqLen the length of the mate sequence
    *  @return the reference segment array
    */
  private def getAlnRegRef(pacLen: Long, pac: Array[Byte], pes: Array[MemPeStat], reg: MemAlnRegType, mateSeqLen: Int): Array[RefType] = {
    var refArray = new Array[RefType](4)

    var r = 0
    while(r < 4) {
      // Check if the direction is skipped
      if(pes(r).failed == 0) {
        var rBeg: Long = -1
        var rEnd: Long = -1
        var len: Long = 0

        var isRev = 0
        if((r >> 1) != (r & 1)) isRev = 1   // whether to reverse complement the mate

        var isLarger = 1
        if((r >> 1) > 0) isLarger = 0   // whether the mate has larger coordinate

        if(isRev == 0) {
          if(isLarger > 0) rBeg = reg.rBeg + pes(r).low
          else rBeg = reg.rBeg - pes(r).high

          if(isLarger > 0) rEnd = reg.rBeg + pes(r).high + mateSeqLen // if on the same strand, end position should be larger to make room for the seq length
          else rEnd = reg.rBeg - pes(r).low + mateSeqLen
        }
        else {
          if(isLarger > 0) rBeg = reg.rBeg + pes(r).low - mateSeqLen // similarly on opposite strands
          else rBeg = reg.rBeg - pes(r).high - mateSeqLen

          if(isLarger > 0) rEnd = reg.rBeg + pes(r).high
          else rEnd = reg.rBeg - pes(r).low
        }

        if(rBeg < 0) rBeg = 0
        if(rEnd > (pacLen << 1)) rEnd = pacLen << 1

        val ret = bnsGetSeq(pacLen, pac, rBeg, rEnd)
        refArray(r) = new RefType
        refArray(r).ref = ret._1
        refArray(r).len = ret._2
        refArray(r).rBeg = rBeg
        refArray(r).rEnd = rEnd
     
        // debug
        if(refArray(r).rEnd - refArray(r).rBeg != refArray(r).len) 
          println("[Java DEBUG] len " + refArray(r).len + ", rBeg " + refArray(r).rBeg + ", rEnd " + refArray(r).rEnd)
      }
      // This direction is skipped
      else {
        refArray(r) = new RefType
        refArray(r).ref = null
      }

      r += 1
    }
 
    refArray
  }


  /**
    *
    *  Prepare the data required by computing pair-end statistics.
    *  The required data for the reducer can be significantly reduced after memPeStatPrep
    *  Used as a mapper in Spark programming model.
    *
    *  @param opt the input MemOptType object
    *  @param pacLen the length of the PAC array
    *  @return the prepared data for the driver to compute pair-end statistics
    */
  def memPeStatPrep(opt: MemOptType, pacLen: Long, pairEndRead: PairEndReadType): PeStatPrepType = {
    var r: Array[Array[MemAlnRegType]] = new Array[Array[MemAlnRegType]](2)
    var peStatPrep = new PeStatPrepType
    var is: Long = 0
    r(0) = pairEndRead.regs0
    r(1) = pairEndRead.regs1

    if(r(0) != null && r(1) != null) {
      if(calSub(opt, r(0)) <= MIN_RATIO * r(0)(0).score) {
        if(calSub(opt, r(1)) <= MIN_RATIO * r(1)(0).score) {
          // Inline mem_infer_dir
          var r1: Boolean = false
          var r2: Boolean = false
          if(r(0)(0).rBeg >= pacLen) r1 = true
          if(r(1)(0).rBeg >= pacLen) r2 = true

          var rBegLarger = r(1)(0).rBeg
          // rBegLarger is the coordinate of read 2 on the read 1 strand
          if(r1 != r2) rBegLarger = (pacLen << 1) - 1 - r(1)(0).rBeg 
          peStatPrep.dist = (r(0)(0).rBeg - rBegLarger).toInt
          if(rBegLarger > r(0)(0).rBeg) peStatPrep.dist = (rBegLarger - r(0)(0).rBeg).toInt
            
          var cond1 = 1
          if(r1 == r2) cond1 = 0
          var cond2 = 3
          if(rBegLarger > r(0)(0).rBeg) cond2 = 0
            
          peStatPrep.dir = cond1 ^ cond2
        }
      }
    }

    peStatPrep
  }


  /**
    *  Compute MemPeStat at the driver node.
    *  Used after the required data are collected (reducer).
    *
    *  @param opt the input MemOptType object
    *  @param peStatPrepArray the prepared data from mappers for compute pair-end statistics
    *  @param pes the pair-end statistics (output)
    */
  def memPeStatCompute(opt: MemOptType, peStatPrepArray: Array[PeStatPrepType], pes: Array[MemPeStat]) {
    var iSize: Array[Vector[Int]] = new Array[Vector[Int]](4)
    var j = 0
    while(j < 4) {
      iSize(j) = scala.collection.immutable.Vector.empty
      j += 1
    }

    var i = 0
    while(i < peStatPrepArray.size) {
      if(peStatPrepArray(i).dist > 0 && peStatPrepArray(i).dist <= opt.maxIns)
        iSize(peStatPrepArray(i).dir) = iSize(peStatPrepArray(i).dir) :+ peStatPrepArray(i).dist

      i += 1
    }
   
    var d = 0
    while(d < 4) {
      val qInit: Vector[Int] = iSize(d)
      if(qInit.size < MIN_DIR_CNT) {
        if(d == 0)
          println("skip orientation FF as there are not enough pairs")
        else if(d == 1)
          println("skip orientation FR as there are not enough pairs")
        else if(d == 2)
          println("skip orientation RF as there are not enough pairs")
        else if(d == 3)
          println("skip orientation RR as there are not enough pairs")
        pes(d).failed = 1
        d += 1
      } 
      else {
        println("analyzing insert size distribution for orientation")
        val q = qInit.sortWith(_.compareTo(_) < 0) // ks_introsort_64
        var p25: Int = q((0.25 * iSize(d).size + 0.499).toInt)
        var p50: Int = q((0.50 * iSize(d).size + 0.499).toInt)
        var p75: Int = q((0.75 * iSize(d).size + 0.499).toInt)
        pes(d).low = (p25 - OUTLIER_BOUND * (p75 - p25) + 0.499).toInt
        if(pes(d).low < 1) pes(d).low = 1
        pes(d).high = (p75 + OUTLIER_BOUND * (p75 - p25) + 0.499).toInt
        println("(25, 50, 75) percentile: (" + p25 + ", " + p50 + ", " + p75 + ")")
        println("low and high boundaries for computing mean and std.dev: (" + pes(d).low + ", " + pes(d).high + ")")

        i = 0
        var x = 0
        pes(d).avg = 0
        while(i < q.size) {
          if(q(i) >= pes(d).low && q(i) <= pes(d).high) {
            pes(d).avg += q(i)
            x += 1
          }
          i += 1
        }
        pes(d).avg /= x
        
        i = 0
        pes(d).std = 0
        while(i < q.size) {
          if(q(i) >= pes(d).low && q(i) <= pes(d).high)
            pes(d).std += (q(i) - pes(d).avg) * (q(i) - pes(d).avg)

          i += 1
        }
        pes(d).std = sqrt(pes(d).std / x)
        println("mean and std.dev: (" + pes(d).avg + ", " + pes(d).std + ")")

        pes(d).low = (p25 - MAPPING_BOUND * (p75 - p25) + .499).toInt
        pes(d).high = (p75 + MAPPING_BOUND * (p75 - p25) + .499).toInt
        if(pes(d).low > pes(d).avg - MAX_STDDEV * pes(d).std) pes(d).low = (pes(d).avg - MAX_STDDEV * pes(d).std + .499).toInt
        if(pes(d).high < pes(d).avg - MAX_STDDEV * pes(d).std) pes(d).high = (pes(d).avg - MAX_STDDEV * pes(d).std + .499).toInt
        if(pes(d).low < 1) pes(d).low = 1
        println("low and high boundaries for proper pairs: (" + pes(d).low + ", " + pes(d).high + ")")

        d += 1
      }
      
    } 

    d = 0
    var max = 0
    while(d < 4) {
      if(max < iSize(d).size) max = iSize(d).size
      d += 1
    }

    d = 0
    while(d < 4) {
      if(pes(d).failed == 0 && iSize(d).size < max * MIN_DIR_RATIO) {
        pes(d).failed = 1
        if(d == 0)
          println("skip orientation FF")
        else if(d == 1)
          println("skip orientation FR")
        else if(d == 2)
          println("skip orientation RF")
        else if(d == 3)
          println("skip orientation RR")
      }     

      d += 1
    }
  }
   

  /**
    *  Perform mate-SW algorithm.
    *  Prepare data for pairing single-end hits.
    *  Used before the batched processing (pure Java version, no JNI involved).
    *
    *  @param opt the input MemOptType object
    *  @param pacLen the length of the PAC array
    *  @param pac the PAC array
    *  @param pes the pair-end statistics (output)
    *  @param reg the alignment
    *  @param mateSeqLen the length of the mate sequence
    *  @param mateRegs the alignments of the mate sequence
    *  @param refArray the reference segment array
    *  @return (the number of added new alignments from mate-SW, alignment array)
    */
  private def memMateSwPreCompute(opt: MemOptType, pacLen: Long, pes: Array[MemPeStat], reg: MemAlnRegType, 
                                  mateSeqLen: Int, mateSeq: Array[Byte], mateRegs: Array[MemAlnRegType], refArray: Array[RefType]): (Int, Array[MemAlnRegType]) = {
    var mateRegsUpdated: Vector[MemAlnRegType] = scala.collection.immutable.Vector.empty
    var skip: Array[Int] = new Array[Int](4)
    var n = 0
    var regArray = new MemAlnRegArrayType

    var r = 0
    while(r < 4) {
      if(pes(r).failed > 0) skip(r) = 1
      else skip(r) = 0
      r += 1     
    }

    var i = 0
    if(mateRegs != null) {
      while(i < mateRegs.size) { // check which orinentation has been found
        // Inline mem_infer_dir
        var r1: Boolean = false
        var r2: Boolean = false
        if(reg.rBeg >= pacLen) r1 = true
        if(mateRegs(i).rBeg >= pacLen) r2 = true

        var rBegLarger = mateRegs(i).rBeg
        // rBegLarger is the coordinate of read 2 on the read 1 strand
        if(r1 != r2) rBegLarger = (pacLen << 1) - 1 - mateRegs(i).rBeg
        var dist: Int = (reg.rBeg - rBegLarger).toInt
        if(rBegLarger > reg.rBeg) dist = (rBegLarger - reg.rBeg).toInt

        var cond1 = 1
        if(r1 == r2) cond1 = 0
        var cond2 = 3
        if(rBegLarger > reg.rBeg) cond2 = 0

        r = cond1 ^ cond2      
        if(dist >= pes(r).low && dist <= pes(r).high) skip(r) = 1     

        i += 1
      }
    }
 
    if(skip(0) + skip(1) + skip(2) + skip(3) == 4) (0, mateRegs)   // consistent pair exist; no need to perform SW

    // NOTE: This copy may be slow!!! May need to be modified!!!
    if(mateRegs != null) {
      i = 0
      while(i < mateRegs.size) {
        mateRegsUpdated = mateRegsUpdated :+ mateRegs(i)
        i += 1
      }
    }

    r = 0
    while(r < 4) {
      if(skip(r) == 0) {
        var seq: Array[Byte] = mateSeq
        var len: Long = 0

        var isRev = 0
        if((r >> 1) != (r & 1)) isRev = 1   // whether to reverse complement the mate

        var isLarger = 1
        if((r >> 1) > 0) isLarger = 0   // whether the mate has larger coordinate

        if(isRev > 0) {
          var rev: Array[Byte] = new Array[Byte](mateSeqLen)
          i = 0
          while(i < mateSeqLen) {
            if(mateSeq(i) < 4) rev(mateSeqLen - 1 - i) = (3 - mateSeq(i)).toByte
            else rev(mateSeqLen - 1 - i) = 4
            i += 1
          }
          seq = rev
        }
        
        if(refArray(r).len == refArray(r).rEnd - refArray(r).rBeg) { // no funny things happening 
          var xtraTmp = 0
          if(mateSeqLen * opt.a < 250) xtraTmp = KSW_XBYTE
          val xtra = KSW_XSUBO | KSW_XSTART | xtraTmp | (opt.minSeedLen * opt.a)
          val aln = SWAlign2(mateSeqLen, seq, refArray(r).len.toInt, refArray(r).ref, 5, opt, xtra) 
          
          var alnTmp = new MemAlnRegType
          if(aln.score >= opt.minSeedLen && aln.qBeg >= 0) { // something goes wrong if aln.qBeg < 0
            if(isRev > 0) {
              alnTmp.qBeg = mateSeqLen - (aln.qEnd + 1)
              alnTmp.qEnd = mateSeqLen - aln.qBeg
              alnTmp.rBeg = (pacLen << 1) - (refArray(r).rBeg + aln.tEnd + 1)
              alnTmp.rEnd = (pacLen << 1) - (refArray(r).rBeg + aln.tBeg)
            }
            else {
              alnTmp.qBeg = aln.qBeg
              alnTmp.qEnd = aln.qEnd + 1
              alnTmp.rBeg = refArray(r).rBeg + aln.tEnd + 1
              alnTmp.rEnd = refArray(r).rBeg + aln.tEnd + 1
            }

            alnTmp.score = aln.score
            alnTmp.csub = aln.scoreSecond
            alnTmp.secondary = -1

            if(alnTmp.rEnd - alnTmp.rBeg < alnTmp.qEnd - alnTmp.qBeg) alnTmp.seedCov = ((alnTmp.rEnd - alnTmp.rBeg) >>> 1).toInt
            else alnTmp.seedCov = (alnTmp.qEnd - alnTmp.qBeg) >>> 1
            
            // move b s.t. ma is sorted
            mateRegsUpdated = mateRegsUpdated :+ alnTmp
          }
          
          n += 1
        }

        if(n > 0) {
          // Sort here!
          // May be more efficient by using insertion sort?
          mateRegsUpdated = mateRegsUpdated.sortBy(seq => (seq.score))
          regArray.regs = mateRegsUpdated.toArray
          regArray.curLength = mateRegsUpdated.size
          regArray.maxLength = mateRegsUpdated.size
          regArray = memSortAndDedup(regArray, opt.maskLevelRedun)
          
        }
      }

      r += 1
    }

    if(n > 0) (n, regArray.regs)
    else (n, mateRegs)
  }


  /**
    *
    *  Prepare the reference segments before sending segments to native C Mate-SW implementation through JNI for batched processing.
    *  This function can be also used in batched Java version.
    *  Used in both memSamPeGroupJNI() and memSamPeGroup()
    *
    *  @param opt the input MemOptType object
    *  @param bns the BNTSeqType object
    *  @param pac the PAC array
    *  @param pes the pair-end statistics (output)
    *  @param groupSize the number of reads to be processed in this group
    *  @param alnRegVecPairs the alignments packed in a paired array
    *  @param seqsPairs the reads packed in a paired array
    *  @return (Reference segment array, selected alignment vector)
    */
  private def memSamPeGroupPrepare(opt: MemOptType, bns: BNTSeqType,  pac: Array[Byte], pes: Array[MemPeStat], groupSize: Int,
                                   alnRegVecPairs: Array[Array[Array[MemAlnRegType]]], seqsPairs: Array[Array[FASTQRecord]]): 
                                   (Array[Array[Array[Array[RefType]]]], Array[Array[Vector[MemAlnRegType]]]) = {
    var regRefArray: Array[Array[Array[Array[RefType]]]] = new Array[Array[Array[Array[RefType]]]](groupSize)
    var alnRegTmpVecPairs: Array[Array[Vector[MemAlnRegType]]] = new Array[Array[Vector[MemAlnRegType]]](groupSize)

    var k = 0
    // Array index -
    // k: the group id
    // i: end id (left end or right end)
    // j: the alignment id
    // r: the direction id
    if((opt.flag & MEM_F_NO_RESCUE) == 0) { // then perform SW for the best alignment

      while(k < groupSize) {     
        regRefArray(k) = new Array[Array[Array[RefType]]](2)
        alnRegTmpVecPairs(k) = new Array[Vector[MemAlnRegType]](2)

        var i = 0
        var alnRegTmpVec = new Array[Vector[MemAlnRegType]](2)
        alnRegTmpVec(0) = scala.collection.immutable.Vector.empty
        alnRegTmpVec(1) = scala.collection.immutable.Vector.empty

        while(i < 2) {
          if(alnRegVecPairs(k)(i) != null) {
            var j = 0
            while(j < alnRegVecPairs(k)(i).size) {
              if(alnRegVecPairs(k)(i)(j).score >= alnRegVecPairs(k)(i)(0).score - opt.penUnpaired)
                alnRegTmpVec(i) = alnRegTmpVec(i) :+ alnRegVecPairs(k)(i)(j)
              j += 1
            }
          }

          i += 1
        }


        i = 0
        while(i < 2) {
          regRefArray(k)(i) = new Array[Array[RefType]](alnRegTmpVec(i).size)
          var j = 0
          while(j < alnRegTmpVec(i).size && j < opt.maxMatesw) {
            regRefArray(k)(i)(j) = new Array[RefType](4)
            var iBar = 0
            if(i == 0) iBar = 1
            regRefArray(k)(i)(j) = getAlnRegRef(bns.l_pac, pac, pes, alnRegTmpVec(i)(j), seqsPairs(k)(iBar).seqLength)
            j += 1
          }

          i += 1
        }   

        alnRegTmpVecPairs(k)(0) = alnRegTmpVec(0)
        alnRegTmpVecPairs(k)(1) = alnRegTmpVec(1)

        k += 1
      } 

    }

    (regRefArray, alnRegTmpVecPairs)
  }


  /**
    *
    *  Perform Batched Mate-SW (Java version)
    *
    *  @param opt the input MemOptType object
    *  @param pacLen the length of the PAC array
    *  @param pes the pair-end statistics (output)
    *  @param groupSize the number of reads to be processed in this group
    *  @param seqsPairs the reads packed in a paired array
    *  @param seqsTransPairs the transformed reads packed in a paired array
    *  @param regRefArray the reference segment array
    *  @param alnRegVecPairs the alignments packed in a paired array
    *  @param alnRegTmpVecPairs the temporary alignments data packed in a paired vector
    *  @return the number of added new alignments from mate-SW
    */
  private def memSamPeGroupMateSW(opt: MemOptType, pacLen: Long, pes: Array[MemPeStat], groupSize: Int, 
                                  seqsPairs: Array[Array[FASTQRecord]], seqsTransPairs: Array[Array[Array[Byte]]], regRefArray: Array[Array[Array[Array[RefType]]]],
                                  alnRegVecPairs: Array[Array[Array[MemAlnRegType]]], alnRegTmpVecPairs: Array[Array[Vector[MemAlnRegType]]]): Int = {
    var k = 0
    var n = 0

    // Array index -
    // k: the group id
    // i: end id (left end or right end)
    // j: the alignment id
    if((opt.flag & MEM_F_NO_RESCUE) == 0) { // then perform SW for the best alignment
      while(k < groupSize) {
    
        var i = 0
        while(i < 2) {
          var j = 0
          while(j < alnRegTmpVecPairs(k)(i).size && j < opt.maxMatesw) {
            var iBar = 0
            if(i == 0) iBar = 1
            val ret = memMateSwPreCompute(opt, pacLen, pes, alnRegTmpVecPairs(k)(i)(j), seqsPairs(k)(iBar).seqLength, seqsTransPairs(k)(iBar), alnRegVecPairs(k)(iBar), regRefArray(k)(i)(j)) 
            n += ret._1
            alnRegVecPairs(k)(iBar) = ret._2
            j += 1
          }

          i += 1
        }  

        k += 1

      }
    }

    n 
  }


  /**
    *  Finish the rest of computation after pair-end SW: pair-end alignments to SAM format 
    *  Used for batched processing. (pure Java execution)
    *  This function can be used for both (1) pure Java batched processing, and (2) JNI batched processing
    *
    *  @param opt the input MemOptType object
    *  @param bns the BNTSeqType object
    *  @param pac the PAC array
    *  @param pes the pair-end statistics (output)
    *  @param groupSize the current read group (the size depends on the data written in Parquet format)
    *  @param id the current read id
    *  @param seqsPairs the input pair-end reads in this group (from Parquet/Avro data format)
    *  @param seqsTransPairs the byte arrays of the input pair-end sequences
    *  @param alnRegVecPairs the alignments of the input pair-end reads in this group
    *  @param isSAMStrOutput whether we output SAM or not
    *  @param samStringArray the output SAM string array
    *  @param samHeader the SAM header required to output SAM strings
    */
  private def memSamPeGroupRest(opt: MemOptType, bns: BNTSeqType,  pac: Array[Byte], pes: Array[MemPeStat], groupSize: Int, id: Long,
                                seqsPairs: Array[Array[FASTQRecord]], seqsTransPairs: Array[Array[Array[Byte]]], alnRegVecPairs: Array[Array[Array[MemAlnRegType]]],
                                isSAMStrOutput: Boolean, samStringArray: Array[Array[String]], samHeader: SAMHeader) {
    var k = 0
    var sum = 0
    // Array index -
    // k: the group id
    // i: end id (left end or right end)
    // j: the alignment id
    while(k < groupSize) {

      var z: Array[Int] = new Array[Int](2)
      var subo: Int = 0
      var numSub: Int = 0
      var extraFlag: Int = 1
      var seqsTrans: Array[Array[Byte]] = new Array[Array[Byte]](2)
      var alnRegVec: Array[Array[MemAlnRegType]] = new Array[Array[MemAlnRegType]](2)
      var seqs: Array[FASTQRecord] = new Array[FASTQRecord](2)
      var noPairingFlag: Boolean = false

      seqs(0) = seqsPairs(k)(0)
      seqs(1) = seqsPairs(k)(1)
      seqsTrans(0) = seqsTransPairs(k)(0)
      seqsTrans(1) = seqsTransPairs(k)(1)
      alnRegVec(0) = alnRegVecPairs(k)(0)
      alnRegVec(1) = alnRegVecPairs(k)(1)      

      //println("id: " + (id + k))
      alnRegVec(0) = memMarkPrimarySe(opt, alnRegVec(0), (id + k)<<1|0)  // id -> id + k
      alnRegVec(1) = memMarkPrimarySe(opt, alnRegVec(1), (id + k)<<1|1)  // id -> id + k

      if((opt.flag & MEM_F_NOPAIRING) == 0) {
        var n = 0
        // pairing single-end hits
        var a0Size = 0
        var a1Size = 0
        if(alnRegVec(0) != null) a0Size = alnRegVec(0).size
        if(alnRegVec(1) != null) a1Size = alnRegVec(1).size

        if(alnRegVec(0) != null && alnRegVec(1) != null) {
          val retVal = memPair(opt, bns.l_pac, pes, alnRegVec, id + k)  // id -> id + k
          val ret = retVal._1
          subo = retVal._2
          numSub = retVal._3
          z = retVal._4

          if(ret > 0) {
            var scoreUn: Int = 0
            var isMulti: Array[Boolean] = new Array[Boolean](2)
            var qPe: Int = 0
            var qSe: Array[Int] = new Array[Int](2)

            var i = 0
            while(i < 2) {
              var j = 1
              var isBreak = false
              while(j < alnRegVec(i).size && !isBreak) {
                if(alnRegVec(i)(j).secondary < 0 && alnRegVec(i)(j).score >= opt.T) 
                  isBreak = true
                else 
                  j += 1
              }
                  
              if(j < alnRegVec(i).size) isMulti(i) = true
              else isMulti(i) = false

              i += 1
            }

            if(!isMulti(0) && !isMulti(1)) {  
              // compute mapQ for the best SE hit
              scoreUn = alnRegVec(0)(0).score + alnRegVec(1)(0).score - opt.penUnpaired
              if(subo < scoreUn) subo = scoreUn
              // Inline raw_mapq(ret - subo, opt.a)
              // #define raw_mapq(diff, a) ((int)(6.02 * (diff) / (a) + .499))
              qPe = (6.02 * (ret - subo) / opt.a + 0.499).toInt
            
              if(numSub > 0) qPe -= (4.343 * log(numSub + 1) + .499).toInt
              if(qPe < 0) qPe = 0
              if(qPe > 60) qPe = 60
  
              // the following assumes no split hits
              if(ret > scoreUn) { // paired alignment is preferred
                var tmpRegs = new Array[MemAlnRegType](2)
                tmpRegs(0) = alnRegVec(0)(z(0))
                tmpRegs(1) = alnRegVec(1)(z(1))

                var i = 0
                while(i < 2) {
                  if(tmpRegs(i).secondary >= 0) {
                    tmpRegs(i).sub = alnRegVec(i)(tmpRegs(i).secondary).score
                    tmpRegs(i).secondary = -1
                  }
  
                  qSe(i) = memApproxMapqSe(opt, tmpRegs(i))
                  i += 1
                }

                if(qSe(0) < qPe) {
                  if(qPe < qSe(0) + 40) qSe(0) = qPe
                  else qSe(0) = qSe(0) + 40
                }

                if(qSe(1) < qPe) {
                  if(qPe < qSe(1) + 40) qSe(1) = qPe
                  else qSe(1) = qSe(1) + 40
                }
              
                extraFlag |= 2
                           
                // cap at the tandem repeat score
                // Inline raw_mapq(tmpRegs(0).score - tmpRegs(0).csub, opt.a)
                var tmp = (6.02 * (tmpRegs(0).score - tmpRegs(0).csub) / opt.a + 0.499).toInt
                if(qSe(0) > tmp) qSe(0) = tmp
                // Inline raw_mapq(tmpRegs(1).score - tmpRegs(1).csub, opt.a)
                tmp = (6.02 * (tmpRegs(1).score - tmpRegs(1).csub) / opt.a + 0.499).toInt
                if(qSe(1) > tmp) qSe(1) = tmp
              }
              else { // the unpaired alignment is preferred
                z(0) = 0
                z(1) = 0
                qSe(0) = memApproxMapqSe(opt, alnRegVec(0)(0))
                qSe(1) = memApproxMapqSe(opt, alnRegVec(1)(0))
              }
            
              // write SAM
              val aln0 = memRegToAln(opt, bns, pac, seqs(0).seqLength, seqsTrans(0), alnRegVec(0)(z(0))) 
              aln0.mapq = qSe(0).toShort
              aln0.flag |= 0x40 | extraFlag
              val aln1 = memRegToAln(opt, bns, pac, seqs(1).seqLength, seqsTrans(1), alnRegVec(1)(z(1)))
              aln1.mapq = qSe(1).toShort
              aln1.flag |= 0x80 | extraFlag
  
              var samStr0 = new SAMString
              var alnList0 = new Array[MemAlnType](1)
              alnList0(0) = aln0
              memAlnToSAM(bns, seqs(0), seqsTrans(0), alnList0, 0, aln1, samHeader, samStr0)
              var samStr1 = new SAMString
              var alnList1 = new Array[MemAlnType](1)
              alnList1(0) = aln1
              memAlnToSAM(bns, seqs(1), seqsTrans(1), alnList1, 0, aln0, samHeader, samStr1)

              // Output format handling
              // Format 1: SAM string ollected the and driver node
              if(isSAMStrOutput) {
                samStringArray(k)(0) = samStr0.str.dropRight(samStr0.size - samStr0.idx).mkString
                samStringArray(k)(1) = samStr1.str.dropRight(samStr1.size - samStr1.idx).mkString
              }

              if(seqs(0).name != seqs(1).name) println("[Error] paired reads have different names: " + seqs(0).name + ", " + seqs(1).name)
            
            }
            else // else: goto no_pairing (TODO: in rare cases, the true hit may be long but with low score)
              noPairingFlag = true
          }
          else // else: goto no_pairing
            noPairingFlag = true
        }
        else // else: goto no_pairing 
          noPairingFlag = true 
      }
      else // else: goto no_pairing
        noPairingFlag = true

      if(noPairingFlag) { // no_pairing: start from here
        var alnVec: Array[MemAlnType] = new Array[MemAlnType](2)

        var i = 0
        while(i < 2) {
          if(alnRegVec(i) != null && alnRegVec(i).size > 0 && alnRegVec(i)(0).score >= opt.T) alnVec(i) = memRegToAln(opt, bns, pac, seqs(i).seqLength, seqsTrans(i), alnRegVec(i)(0)) 
          else alnVec(i) = memRegToAln(opt, bns, pac, seqs(i).seqLength, seqsTrans(i), null) 

          i += 1
        }

        // if the top hits from the two ends constitute a proper pair, flag it.
        if((opt.flag & MEM_F_NOPAIRING) == 0 && alnVec(0).rid == alnVec(1).rid && alnVec(0).rid >= 0) {
          // Inline mem_infer_dir
          var r1: Boolean = false
          var r2: Boolean = false
          if(alnRegVec(0)(0).rBeg >= bns.l_pac) r1 = true
          if(alnRegVec(1)(0).rBeg >= bns.l_pac) r2 = true

          var rBegLarger = alnRegVec(1)(0).rBeg
          // rBegLarger is the coordinate of read 2 on the read 1 strand
          if(r1 != r2) rBegLarger = (bns.l_pac << 1) - 1 - alnRegVec(1)(0).rBeg
          var dist: Int = (alnRegVec(0)(0).rBeg - rBegLarger).toInt
          if(rBegLarger > alnRegVec(0)(0).rBeg) dist = (rBegLarger - alnRegVec(0)(0).rBeg).toInt

          var cond1 = 1
          if(r1 == r2) cond1 = 0
          var cond2 = 3
          if(rBegLarger > alnRegVec(0)(0).rBeg) cond2 = 0

          var d = cond1 ^ cond2
       
          if(pes(d).failed == 0 && dist >= pes(d).low && dist <= pes(d).high) extraFlag |= 2 
        }

        val samStr0 = memRegToSAMSe(opt, bns, pac, seqs(0), seqsTrans(0), alnRegVec(0), 0x41 | extraFlag, alnVec(1), samHeader)
        val samStr1 = memRegToSAMSe(opt, bns, pac, seqs(1), seqsTrans(1), alnRegVec(1), 0x81 | extraFlag, alnVec(0), samHeader)

        // Output format handling
        // Format 1: SAM string ollected the and driver node
        if(isSAMStrOutput) {
          samStringArray(k)(0) = samStr0
          samStringArray(k)(1) = samStr1
        }

        if(seqs(0).name != seqs(1).name) println("[Error] paired reads have different names: " + seqs(0).name + ", " + seqs(1).name)
      }

      k += 1
    }

  }

  
  /**
    *  Initialize the sequence pair for pair-end FASTQRecord
    *
    *  @param seqsPairsIn the input PairEndFASTQRecord arrays
    *  @return the FASTQRecord arrays
    */
  private def initSingleEndPairs(seqsPairsIn: Array[PairEndFASTQRecord]): Array[Array[FASTQRecord]] = {
    var seqsPairs = new Array[Array[FASTQRecord]](seqsPairsIn.size)
    var i = 0
    while(i < seqsPairsIn.size) {
      seqsPairs(i) = new Array[FASTQRecord](2)
      seqsPairs(i)(0) = seqsPairsIn(i).getSeq0
      seqsPairs(i)(1) = seqsPairsIn(i).getSeq1
      i += 1
    }

    seqsPairs
  }

  /**
    *  Intialize the transformed sequence pair arrays 
    *
    *  @param seqsPairs the input FASTQRecord arrays
    *  @param groupSize the number of reads to be processed in this group
    *  @return the transformed sequence byte arrays
    */ 
  private def initSeqsTransPairs(seqsPairs: Array[Array[FASTQRecord]], groupSize: Int): Array[Array[Array[Byte]]] = {
    var seqsTransPairs: Array[Array[Array[Byte]]] = new Array[Array[Array[Byte]]](groupSize)
    var k = 0
    while(k < groupSize) {
      seqsTransPairs(k) = new Array[Array[Byte]](2)
      val seqStr0 = new String(seqsPairs(k)(0).seq.array)
      seqsTransPairs(k)(0) = seqStr0.toCharArray.map(ele => locusEncode(ele))
      val seqStr1 = new String(seqsPairs(k)(1).seq.array)
      seqsTransPairs(k)(1) = seqStr1.toCharArray.map(ele => locusEncode(ele))
      k += 1
    }

    seqsTransPairs
  }


  /**
    *  memSamPeGroup: Pair-end alignments to SAM format 
    *  Used for batched processing. (pure Java version)
    *
    *  @param opt the input MemOptType object
    *  @param bns the BNTSeqType object
    *  @param pac the PAC array
    *  @param pes the pair-end statistics (output)
    *  @param groupSize the current read group (the size depends on the data written in Parquet format)
    *  @param id the current read id
    *  @param seqsPairsIn the input pair-end reads in this group (from Parquet/Avro data format)
    *  @param alnRegVecPairs the alignments of the input pair-end reads in this group
    *  @param isSAMStrOutput whether we output SAM or not
    *  @param samStringArray the output SAM string array
    *  @param samHeader the SAM header required to output SAM strings
    */
  def memSamPeGroup(opt: MemOptType, bns: BNTSeqType, pac: Array[Byte], pes: Array[MemPeStat], groupSize: Int, id: Long, 
                    seqsPairsIn: Array[PairEndFASTQRecord], alnRegVecPairs: Array[Array[Array[MemAlnRegType]]], 
                    isSAMStrOutput: Boolean, samStringArray: Array[Array[String]], samHeader: SAMHeader) {

    var seqsPairs = initSingleEndPairs(seqsPairsIn)
    var seqsTransPairs = initSeqsTransPairs(seqsPairs, groupSize)

    //println("memSamPeGroupPrepare")
    val prepRet = memSamPeGroupPrepare(opt, bns, pac, pes, groupSize, alnRegVecPairs, seqsPairs)

    //println("memSamPeGroupMateSW")
    val n = memSamPeGroupMateSW(opt, bns.l_pac, pes, groupSize, seqsPairs, seqsTransPairs, prepRet._1, alnRegVecPairs, prepRet._2)
    //println("memSamRest")
    memSamPeGroupRest(opt, bns,  pac, pes, groupSize, id, seqsPairs, seqsTransPairs, alnRegVecPairs, isSAMStrOutput, samStringArray, samHeader)
    //println("memSamPeGroup done in this batch!")
  }


  /******************************************************************************************************************/
  /**********************     END of batched version (batched processing, pure Java)     ****************************/
  /******************************************************************************************************************/


  /******************************************************************************************************************/
  /*************     MemSamPe.scala: batched JNI version (batched processing, native C library)   *******************/
  /******************************************************************************************************************/


  /**
    *  WARNING: obsolete
    *  Please use memSamPeGroupJNIPrepare for better efficiency
    *  Prepare JNI data. Transform the data into the format defined in JNI 
    *  Used for JNI batched processing.
    *
    *  @param groupSize the current read group (the size depends on the data written in Parquet format)
    *  @param maxMatesw the input parameter described in the MemOptType object
    *  @param seqsPairs the input pair-end reads in this group (from Parquet/Avro data format)
    *  @param seqsTransPairs the byte arrays of the input pair-end sequences
    *  @param refArray the four-dimensional input array for reference segments
    *  @param alnRegArray the alignments after finding more pair-end reads and filtering after using the maxMatesw parameter
    *  @param alnRegVecPairs the alignments of the input pair-end reads in this group
    *  @return (mateSWArray, seqsSWArray, refSWArray, refSWArraySize): the four arrays used in native library to be passed through JNI
    */
  private def memSamPeJNIPrep(groupSize: Int, maxMatesw: Int, seqsPairs: Array[Array[FASTQRecord]], seqsTransPairs: Array[Array[Array[Byte]]], refArray: Array[Array[Array[Array[RefType]]]], 
    alnRegArray: Array[Array[Vector[MemAlnRegType]]], alnRegVecPairs: Array[Array[Array[MemAlnRegType]]]): (Array[MateSWType], Array[SeqSWType], Array[RefSWType], Array[Int]) = {

    var mateSWVec: Vector[MateSWType] = scala.collection.immutable.Vector.empty
    var seqsSWVec: Vector[SeqSWType] = scala.collection.immutable.Vector.empty
    var refSWVec: Vector[RefSWType] = scala.collection.immutable.Vector.empty
    var refSWArraySize: Array[Int] = new Array[Int](groupSize * 2)

    // Array index -
    // k: the group id
    // i: end id (left end or right end)
    // j: the alignment id
    var k = 0
    while(k < groupSize) {
      var i = 0
      while(i < 2) {
        var seq = new SeqSWType
        seq.readIdx = k
        seq.pairIdx = i
        seq.seqLength = seqsPairs(k)(i).seqLength
        seq.seqTrans = seqsTransPairs(k)(i)
        seqsSWVec = seqsSWVec :+ seq

        if(alnRegArray(k)(i).size > maxMatesw)
          refSWArraySize(k * 2 + i) = maxMatesw
        else
          refSWArraySize(k * 2 + i) = alnRegArray(k)(i).size

        var j = 0
        if(alnRegVecPairs(k)(i) != null) {
          while(j < alnRegVecPairs(k)(i).size) {
            var mateSW = new MateSWType
            mateSW.readIdx = k
            mateSW.pairIdx = i
            mateSW.regIdx = j
            mateSW.alnReg = alnRegVecPairs(k)(i)(j)
            mateSWVec = mateSWVec :+ mateSW
            j += 1
          }
        }

        j = 0
        while(j < refSWArraySize(k * 2 + i)) {
          var refSW = new RefSWType
          refSW.readIdx = k
          refSW.pairIdx = i
          refSW.regIdx = j
          refSW.rBegArray = new Array[Long](4)
          refSW.rEndArray = new Array[Long](4)
          refSW.lenArray = new Array[Long](4)
          refSW.rBegArray(0) = refArray(k)(i)(j)(0).rBeg
          refSW.rBegArray(1) = refArray(k)(i)(j)(1).rBeg
          refSW.rBegArray(2) = refArray(k)(i)(j)(2).rBeg
          refSW.rBegArray(3) = refArray(k)(i)(j)(3).rBeg
          refSW.rEndArray(0) = refArray(k)(i)(j)(0).rEnd
          refSW.rEndArray(1) = refArray(k)(i)(j)(1).rEnd
          refSW.rEndArray(2) = refArray(k)(i)(j)(2).rEnd
          refSW.rEndArray(3) = refArray(k)(i)(j)(3).rEnd
          refSW.lenArray(0) = refArray(k)(i)(j)(0).len
          refSW.lenArray(1) = refArray(k)(i)(j)(1).len
          refSW.lenArray(2) = refArray(k)(i)(j)(2).len
          refSW.lenArray(3) = refArray(k)(i)(j)(3).len
          refSW.ref0 = refArray(k)(i)(j)(0).ref
          refSW.ref1 = refArray(k)(i)(j)(1).ref
          refSW.ref2 = refArray(k)(i)(j)(2).ref
          refSW.ref3 = refArray(k)(i)(j)(3).ref
          refSWVec = refSWVec :+ refSW
          j += 1
        }

        i += 1
      }
      
      k += 1
    }

    val mateSWArray = mateSWVec.toArray
    //println("mateSWArray size: " + mateSWArray.size)
    val seqsSWArray = seqsSWVec.toArray
    //println("seqsSWArray size: " + seqsSWArray.size)
    val refSWArray = refSWVec.toArray
    //println("refSWArray size: " + refSWArray.size)

   (mateSWArray, seqsSWArray, refSWArray, refSWArraySize)
  }


  /**
    *  Get the reference segments, including rBeg, rEnd, ref (reference segment), and len.
    *  Used before the JNI batched processing.
    *
    *  @param pacLen the length of the PAC array
    *  @param pac the PAC array
    *  @param pes the pair-end statistics (output)
    *  @param reg the alignment
    *  @param mateSeqLen the length of the mate sequence
    *  @param readIdx the read index
    *  @param pairIdx the pair-end index (0/1)
    *  @param regIdx the alignment index
    *  @return the reference segment object used in JNI
    */
  private def getAlnRegRefJNI(pacLen: Long, pac: Array[Byte], pes: Array[MemPeStat], reg: MemAlnRegType, mateSeqLen: Int, readIdx: Int, pairIdx: Int, regIdx: Int): RefSWType = {
    var refByteArray = new Array[Array[Byte]](4)
    var refSW = new RefSWType
    refSW.readIdx = readIdx
    refSW.pairIdx = pairIdx
    refSW.regIdx = regIdx
    refSW.rBegArray = new Array[Long](4)
    refSW.rEndArray = new Array[Long](4)
    refSW.lenArray = new Array[Long](4)

    var r = 0
    while(r < 4) {
      // Check if the direction is skipped
      if(pes(r).failed == 0) {
        var rBeg: Long = -1
        var rEnd: Long = -1
        var len: Long = 0

        var isRev = 0
        if((r >> 1) != (r & 1)) isRev = 1   // whether to reverse complement the mate

        var isLarger = 1
        if((r >> 1) > 0) isLarger = 0   // whether the mate has larger coordinate

        if(isRev == 0) {
          if(isLarger > 0) rBeg = reg.rBeg + pes(r).low
          else rBeg = reg.rBeg - pes(r).high

          if(isLarger > 0) rEnd = reg.rBeg + pes(r).high + mateSeqLen // if on the same strand, end position should be larger to make room for the seq length
          else rEnd = reg.rBeg - pes(r).low + mateSeqLen
        }
        else {
          if(isLarger > 0) rBeg = reg.rBeg + pes(r).low - mateSeqLen // similarly on opposite strands
          else rBeg = reg.rBeg - pes(r).high - mateSeqLen

          if(isLarger > 0) rEnd = reg.rBeg + pes(r).high
          else rEnd = reg.rBeg - pes(r).low
        }

        if(rBeg < 0) rBeg = 0
        if(rEnd > (pacLen << 1)) rEnd = pacLen << 1

        val ret = bnsGetSeq(pacLen, pac, rBeg, rEnd)
        refByteArray(r) = ret._1
        refSW.lenArray(r) = ret._2
        refSW.rBegArray(r) = rBeg
        refSW.rEndArray(r) = rEnd
     
        // debug
        if(refSW.rEndArray(r) - refSW.rBegArray(r) != refSW.lenArray(r)) 
          println("[Java DEBUG] len " + refSW.lenArray(r) + ", rBeg " + refSW.rBegArray(r) + ", rEnd " + refSW.rEndArray(r))
      }
      // This direction is skipped
      else {
        refByteArray(r) = null 
        refSW.lenArray(r) = 0
        refSW.rBegArray(r) = -1
        refSW.rEndArray(r) = -1
      }

      r += 1
    }
 
    refSW.ref0 = refByteArray(0)
    refSW.ref1 = refByteArray(1)
    refSW.ref2 = refByteArray(2)
    refSW.ref3 = refByteArray(3)
    refSW
  }


  /**
    *  Prepare JNI data. Transform the data into the format defined in JNI 
    *  Used for JNI batched processing.
    *
    *  @param opt the input MemOptType object
    *  @param bns the BNTSeqType object
    *  @param pac the PAC array
    *  @param pes the pair-end statistics (output)
    *  @param groupSize the current read group (the size depends on the data written in Parquet format)
    *  @param seqsPairs the input pair-end reads in this group (from Parquet/Avro data format)
    *  @param seqsTransPairs the byte arrays of the input pair-end sequences
    *  @param alnRegVecPairs the alignments of the input pair-end reads in this group
    *  @return (mateSWArray, seqsSWArray, refSWArray, refSWArraySize): the four arrays used in native library to be passed through JNI
    */
  private def memSamPeGroupJNIPrepare(opt: MemOptType, bns: BNTSeqType,  pac: Array[Byte], pes: Array[MemPeStat], groupSize: Int,
    seqsPairs: Array[Array[FASTQRecord]], seqsTransPairs: Array[Array[Array[Byte]]], alnRegVecPairs: Array[Array[Array[MemAlnRegType]]] 
    ): (Array[MateSWType], Array[SeqSWType], Array[RefSWType], Array[Int]) = {

    var mateSWVec: Vector[MateSWType] = scala.collection.immutable.Vector.empty
    var seqsSWVec: Vector[SeqSWType] = scala.collection.immutable.Vector.empty
    var refSWVec: Vector[RefSWType] = scala.collection.immutable.Vector.empty
    var refSWArraySize: Array[Int] = new Array[Int](groupSize * 2)

    // Array index -
    // k: the group id
    // i: end id (left end or right end)
    // j: the alignment id
    var k = 0
    if((opt.flag & MEM_F_NO_RESCUE) == 0) { 

      while(k < groupSize) {

        var i = 0
        var alnRegTmpVec = new Array[Vector[MemAlnRegType]](2)
        alnRegTmpVec(0) = scala.collection.immutable.Vector.empty
        alnRegTmpVec(1) = scala.collection.immutable.Vector.empty

        while(i < 2) {
          if(alnRegVecPairs(k)(i) != null) {
            var j = 0
            while(j < alnRegVecPairs(k)(i).size) {
              if(alnRegVecPairs(k)(i)(j).score >= alnRegVecPairs(k)(i)(0).score - opt.penUnpaired)
                alnRegTmpVec(i) = alnRegTmpVec(i) :+ alnRegVecPairs(k)(i)(j)
              j += 1
            }
          }

          i += 1
        }

        i = 0
        while(i < 2) {
          var j = 0
          while(j < alnRegTmpVec(i).size && j < opt.maxMatesw) {
            var iBar = 0
            if(i == 0) iBar = 1
            var refSW = new RefSWType
            refSW = getAlnRegRefJNI(bns.l_pac, pac, pes, alnRegTmpVec(i)(j), seqsPairs(k)(iBar).seqLength, k, i, j)
            refSWVec = refSWVec :+ refSW
            j += 1
          }

          // Calculate the reference array size
          if(alnRegTmpVec(i).size > opt.maxMatesw)
            refSWArraySize(k * 2 + i) = opt.maxMatesw
          else
            refSWArraySize(k * 2 + i) = alnRegTmpVec(i).size

          i += 1
        }

        k += 1
      }

    }


    // Array index -
    // k: the group id
    // i: end id (left end or right end)
    // j: the alignment id
		k = 0
    while(k < groupSize) {
      var i = 0
      while(i < 2) {
        var seq = new SeqSWType
        seq.readIdx = k
        seq.pairIdx = i
        seq.seqLength = seqsPairs(k)(i).seqLength
        seq.seqTrans = seqsTransPairs(k)(i)
        seqsSWVec = seqsSWVec :+ seq

        var j = 0
        if(alnRegVecPairs(k)(i) != null) {
          while(j < alnRegVecPairs(k)(i).size) {
            var mateSW = new MateSWType
            mateSW.readIdx = k
            mateSW.pairIdx = i
            mateSW.regIdx = j
            mateSW.alnReg = alnRegVecPairs(k)(i)(j)
            mateSWVec = mateSWVec :+ mateSW
            j += 1
          }
        }

        i += 1
      }
      
      k += 1
    }

    val mateSWArray = mateSWVec.toArray
    println("mateSWArray size: " + mateSWArray.size)
    val seqsSWArray = seqsSWVec.toArray
    println("seqsSWArray size: " + seqsSWArray.size)
    val refSWArray = refSWVec.toArray
    println("refSWArray size: " + refSWArray.size)

    (mateSWArray, seqsSWArray, refSWArray, refSWArraySize)
  }


  /**
    *  Transform the array obtained from JNI back into the data structure used in Java
    *  
    *  @param groupSize the current read group size (the size depends on the data written in Parquet format)
    *  @param inArray the array obtained from JNI
    *  @return the alignment array used in memSamPeGroupRest()
    */
  private def mateSWArrayToAlnRegPairArray(groupSize: Int, inArray: Array[MateSWType]): Array[Array[Array[MemAlnRegType]]] = {
    var outVec: Array[Array[Vector[MemAlnRegType]]] = new Array[Array[Vector[MemAlnRegType]]](groupSize)
    var k = 0
    while(k < groupSize) {
      outVec(k) = new Array[Vector[MemAlnRegType]](2)
      var i = 0
      while(i < 2) {
        outVec(k)(i) = scala.collection.immutable.Vector.empty
        i += 1
      }
      k += 1 
    }

    k = 0
    //println("inArray size: " + inArray.size)
    while(k < inArray.size) {
      outVec(inArray(k).readIdx)(inArray(k).pairIdx) = outVec(inArray(k).readIdx)(inArray(k).pairIdx) :+ inArray(k).alnReg
      k += 1
    }

    var outArray: Array[Array[Array[MemAlnRegType]]] = new Array[Array[Array[MemAlnRegType]]](groupSize)
    k = 0
    while(k < groupSize) {
      outArray(k) = new Array[Array[MemAlnRegType]](2)
      var i = 0
      while(i < 2) {
        outArray(k)(i) = new Array[MemAlnRegType](0)
        outArray(k)(i) = outVec(k)(i).toArray
        i += 1
      }
      k += 1 
    }
  
    outArray
  }


  /**
    *  memSamPeGroupJNI: Pair-end alignments to SAM format 
    *  Used for JNI batched processing. (native C library)
    *
    *  @param opt the input MemOptType object
    *  @param bns the BNTSeqType object
    *  @param pac the PAC array
    *  @param pes the pair-end statistics (output)
    *  @param groupSize the current read group (the size depends on the data written in Parquet format)
    *  @param id the current read id
    *  @param seqsPairsIn the input pair-end reads in this group (from Parquet/Avro data format)
    *  @param alnRegVecPairs the alignments of the input pair-end reads in this group
    *  @param isSAMStrOutput whether we output SAM or not
    *  @param samStringArray the output SAM string array
    *  @param samHeader the SAM header required to output SAM strings
    */
  def memSamPeGroupJNI(opt: MemOptType, bns: BNTSeqType, pac: Array[Byte], pes: Array[MemPeStat], groupSize: Int, id: Long, 
                       seqsPairsIn: Array[PairEndFASTQRecord], alnRegVecPairs: Array[Array[Array[MemAlnRegType]]],
                       isSAMStrOutput: Boolean, samStringArray: Array[Array[String]], samHeader: SAMHeader) {

    // prepare seqsPairs array
    var seqsPairs = initSingleEndPairs(seqsPairsIn)
    // prepare seqsTransPairs array
    var seqsTransPairs = initSeqsTransPairs(seqsPairs, groupSize)

    // obsolete
    //println("memSamPeGroupPrepare")
    //val prepRet = memSamPeGroupPrepare(opt, bns, pac, pes, groupSize, alnRegVecPairs, seqsPairs)
    //val refArray = prepRet._1
    //val alnRegArray = prepRet._2  
    // test JNI, pass array of objects
    //println("JNI data preparation")
    //val ret = memSamPeJNIPrep(groupSize, opt.maxMatesw, seqsPairs, seqsTransPairs, refArray, alnRegArray, alnRegVecPairs)

    // use memSamPeGroupJNIPrepare() to prepare data before calling JNI function
    //println("[New] memSamPeGroupJNIPrepare")
    val ret = memSamPeGroupJNIPrepare(opt, bns,  pac, pes, groupSize, seqsPairs, seqsTransPairs, alnRegVecPairs)
    
    // call JNI function to use native library
    //println("Call JNI")
    val mateSWArray = ret._1
    val seqsSWArray = ret._2
    val refSWArray = ret._3
    val refSWArraySize = ret._4
    val jni = new MateSWJNI
    val retMateSWArray = jni.mateSWJNI(opt, bns.l_pac, pes, groupSize, seqsSWArray, mateSWArray, refSWArray, refSWArraySize)

    // transform the output array obtained from JNI
    //println("mateSWArrayToAlnRegPairArray");
    val alnRegVecPairsJNI = mateSWArrayToAlnRegPairArray(groupSize, retMateSWArray)

    // run the rest of alingment -> SAM function (Smith-Waterman algorithm to generate CIGAR string)
    //println("memSamPeGroupRest")
    memSamPeGroupRest(opt, bns,  pac, pes, groupSize, id, seqsPairs, seqsTransPairs, alnRegVecPairsJNI, isSAMStrOutput, samStringArray, samHeader)

    //println("memSamPeGroup done in this batch!")
  }


  /******************************************************************************************************************/
  /******************     END of batched JNI version (batched processing, native C library)    **********************/
  /******************************************************************************************************************/

  /******************************************************************************************************************/
  /******************************     MemSamPe.scala: functions for ADAM output    **********************************/
  /******************************************************************************************************************/

  /**
    *  memADAMPe: Pair-end alignments to SAM format 
    *  Used for single read without batched processing. (pure Java)
    *  Output ADAM format in the distributed file system.
    *
    *  @param opt the input MemOptType object
    *  @param bns the BNTSeqType object
    *  @param pac the PAC array
    *  @param pes the pair-end statistics (output)
    *  @param id the current read id
    *  @param seqsIn the input pair-end read (from Parquet/Avro data format)
    *  @param alnRegVec the alignments of this read
    *  @param samHeader the SAM header required to output SAM strings
    *  @param seqDict the sequences (chromosome) dictionary: used for ADAM format output
    *  @param readGroup the read group: used for ADAM format output
    *  @return an array of ADAM format object
    */
  def memADAMPe(opt: MemOptType, bns: BNTSeqType, pac: Array[Byte], pes: Array[MemPeStat], id: Long, 
               seqsIn: PairEndFASTQRecord, alnRegVec: Array[Array[MemAlnRegType]], samHeader: SAMHeader, seqDict: SequenceDictionary, readGroup: RecordGroup): Array[AlignmentRecord] = {
    var n: Int = 0
    var z: Array[Int] = new Array[Int](2)
    var subo: Int = 0
    var numSub: Int = 0
    var extraFlag: Int = 1
    var seqsTrans: Array[Array[Byte]] = new Array[Array[Byte]](2)
    var noPairingFlag: Boolean = false
    var adamVec: Vector[AlignmentRecord] = scala.collection.immutable.Vector.empty 

    var seqs = new Array[FASTQRecord](2)
    seqs(0) = seqsIn.getSeq0
    seqs(1) = seqsIn.getSeq1

    val seqStr0 = new String(seqs(0).seq.array)
    seqsTrans(0) = seqStr0.toCharArray.map(ele => locusEncode(ele))
    val seqStr1 = new String(seqs(1).seq.array)
    seqsTrans(1) = seqStr1.toCharArray.map(ele => locusEncode(ele))

    if((opt.flag & MEM_F_NO_RESCUE) == 0) { // then perform SW for the best alignment
      var i = 0
      var alnRegTmpVec = new Array[Vector[MemAlnRegType]](2)
      alnRegTmpVec(0) = scala.collection.immutable.Vector.empty
      alnRegTmpVec(1) = scala.collection.immutable.Vector.empty

      while(i < 2) {
        if(alnRegVec(i) != null) {
          var j = 0
          while(j < alnRegVec(i).size) {
            if(alnRegVec(i)(j).score >= alnRegVec(i)(0).score - opt.penUnpaired)
              alnRegTmpVec(i) = alnRegTmpVec(i) :+ alnRegVec(i)(j)
            j += 1
          }
        }

        i += 1
      }

      i = 0
      while(i < 2) {
        var j = 0
        while(j < alnRegTmpVec(i).size && j < opt.maxMatesw) {
          var iBar = 0
          if(i == 0) iBar = 1
          val ret = memMateSw(opt, bns.l_pac, pac, pes, alnRegTmpVec(i)(j), seqs(iBar).seqLength, seqsTrans(iBar), alnRegVec(iBar)) 
          n += ret._1
          alnRegVec(iBar) = ret._2
          j += 1
        }

        i += 1
      }  
    } 
    
    alnRegVec(0) = memMarkPrimarySe(opt, alnRegVec(0), id<<1|0)
    alnRegVec(1) = memMarkPrimarySe(opt, alnRegVec(1), id<<1|1)

    if((opt.flag & MEM_F_NOPAIRING) == 0) {
      // pairing single-end hits
      var a0Size = 0
      var a1Size = 0
      if(alnRegVec(0) != null) a0Size = alnRegVec(0).size
      if(alnRegVec(1) != null) a1Size = alnRegVec(1).size

      if(alnRegVec(0) != null && alnRegVec(1) != null) {
        val retVal = memPair(opt, bns.l_pac, pes, alnRegVec, id)
        val ret = retVal._1
        subo = retVal._2
        numSub = retVal._3
        z = retVal._4

        if(ret > 0) {
          var scoreUn: Int = 0
          var isMulti: Array[Boolean] = new Array[Boolean](2)
          var qPe: Int = 0
          var qSe: Array[Int] = new Array[Int](2)

          var i = 0
          while(i < 2) {
            var j = 1
            var isBreak = false
            while(j < alnRegVec(i).size && !isBreak) {
              if(alnRegVec(i)(j).secondary < 0 && alnRegVec(i)(j).score >= opt.T) 
                isBreak = true
              else 
                j += 1
            }
              
            if(j < alnRegVec(i).size) isMulti(i) = true
            else isMulti(i) = false

            i += 1
          }

          if(!isMulti(0) && !isMulti(1)) {  
            // compute mapQ for the best SE hit
            scoreUn = alnRegVec(0)(0).score + alnRegVec(1)(0).score - opt.penUnpaired
            if(subo < scoreUn) subo = scoreUn
            // Inline raw_mapq(ret - subo, opt.a)
            // #define raw_mapq(diff, a) ((int)(6.02 * (diff) / (a) + .499))
            qPe = (6.02 * (ret - subo) / opt.a + 0.499).toInt
            
            if(numSub > 0) qPe -= (4.343 * log(numSub + 1) + .499).toInt
            if(qPe < 0) qPe = 0
            if(qPe > 60) qPe = 60

            // the following assumes no split hits
            if(ret > scoreUn) { // paired alignment is preferred
              var tmpRegs = new Array[MemAlnRegType](2)
              tmpRegs(0) = alnRegVec(0)(z(0))
              tmpRegs(1) = alnRegVec(1)(z(1))

              var i = 0
              while(i < 2) {
                if(tmpRegs(i).secondary >= 0) {
                  tmpRegs(i).sub = alnRegVec(i)(tmpRegs(i).secondary).score
                  tmpRegs(i).secondary = -1
                }

                qSe(i) = memApproxMapqSe(opt, tmpRegs(i))
                i += 1
              }

              if(qSe(0) < qPe) {
                if(qPe < qSe(0) + 40) qSe(0) = qPe
                else qSe(0) = qSe(0) + 40
              }

              if(qSe(1) < qPe) {
                if(qPe < qSe(1) + 40) qSe(1) = qPe
                else qSe(1) = qSe(1) + 40
              }
              
              extraFlag |= 2
                           
              // cap at the tandem repeat score
              // Inline raw_mapq(tmpRegs(0).score - tmpRegs(0).csub, opt.a)
              var tmp = (6.02 * (tmpRegs(0).score - tmpRegs(0).csub) / opt.a + 0.499).toInt
              if(qSe(0) > tmp) qSe(0) = tmp
              // Inline raw_mapq(tmpRegs(1).score - tmpRegs(1).csub, opt.a)
              tmp = (6.02 * (tmpRegs(1).score - tmpRegs(1).csub) / opt.a + 0.499).toInt
              if(qSe(1) > tmp) qSe(1) = tmp
            }
            else { // the unpaired alignment is preferred
              z(0) = 0
              z(1) = 0
              qSe(0) = memApproxMapqSe(opt, alnRegVec(0)(0))
              qSe(1) = memApproxMapqSe(opt, alnRegVec(1)(0))
            }
            
            // write ADAM
            val aln0 = memRegToAln(opt, bns, pac, seqs(0).seqLength, seqsTrans(0), alnRegVec(0)(z(0))) 
            aln0.mapq = qSe(0).toShort
            aln0.flag |= 0x40 | extraFlag
            val aln1 = memRegToAln(opt, bns, pac, seqs(1).seqLength, seqsTrans(1), alnRegVec(1)(z(1)))
            aln1.mapq = qSe(1).toShort
            aln1.flag |= 0x80 | extraFlag

            var alnList0 = new Array[MemAlnType](1)
            alnList0(0) = aln0
            adamVec = adamVec :+ memAlnToADAM(bns, seqs(0), seqsTrans(0), alnList0, 0, aln1, samHeader, seqDict, readGroup)
            var alnList1 = new Array[MemAlnType](1)
            alnList1(0) = aln1
            adamVec = adamVec :+ memAlnToADAM(bns, seqs(1), seqsTrans(1), alnList1, 0, aln0, samHeader, seqDict, readGroup)

            // NOTE: temporarily comment out in Spark version
            //if(seqs(0).name != seqs(1).name) println("[Error] paired reads have different names: " + seqs(0).name + ", " + seqs(1).name)
            
          }
          else // else: goto no_pairing (TODO: in rare cases, the true hit may be long but with low score)
            noPairingFlag = true
        }
        else // else: goto no_pairing
          noPairingFlag = true
      }
      else // else: goto no_pairing 
        noPairingFlag = true 
    }
    else // else: goto no_pairing
      noPairingFlag = true

    if(noPairingFlag) { // no_pairing: start from here
      var alnVec: Array[MemAlnType] = new Array[MemAlnType](2)

      var i = 0
      while(i < 2) {
        if(alnRegVec(i) != null && alnRegVec(i).size > 0 && alnRegVec(i)(0).score >= opt.T) alnVec(i) = memRegToAln(opt, bns, pac, seqs(i).seqLength, seqsTrans(i), alnRegVec(i)(0)) 
        else alnVec(i) = memRegToAln(opt, bns, pac, seqs(i).seqLength, seqsTrans(i), null) 

        i += 1
      }

      // if the top hits from the two ends constitute a proper pair, flag it.
      if((opt.flag & MEM_F_NOPAIRING) == 0 && alnVec(0).rid == alnVec(1).rid && alnVec(0).rid >= 0) {
        // Inline mem_infer_dir
        var r1: Boolean = false
        var r2: Boolean = false
        if(alnRegVec(0)(0).rBeg >= bns.l_pac) r1 = true
        if(alnRegVec(1)(0).rBeg >= bns.l_pac) r2 = true

        var rBegLarger = alnRegVec(1)(0).rBeg
        // rBegLarger is the coordinate of read 2 on the read 1 strand
        if(r1 != r2) rBegLarger = (bns.l_pac << 1) - 1 - alnRegVec(1)(0).rBeg
        var dist: Int = (alnRegVec(0)(0).rBeg - rBegLarger).toInt
        if(rBegLarger > alnRegVec(0)(0).rBeg) dist = (rBegLarger - alnRegVec(0)(0).rBeg).toInt

        var cond1 = 1
        if(r1 == r2) cond1 = 0
        var cond2 = 3
        if(rBegLarger > alnRegVec(0)(0).rBeg) cond2 = 0

        var d = cond1 ^ cond2
     
        if(pes(d).failed == 0 && dist >= pes(d).low && dist <= pes(d).high) extraFlag |= 2 
      }

      adamVec = adamVec ++ memRegToADAMSe(opt, bns, pac, seqs(0), seqsTrans(0), alnRegVec(0), 0x41 | extraFlag, alnVec(1), samHeader, seqDict, readGroup)
      adamVec = adamVec ++ memRegToADAMSe(opt, bns, pac, seqs(1), seqsTrans(1), alnRegVec(1), 0x81 | extraFlag, alnVec(0), samHeader, seqDict, readGroup)

      if(seqs(0).name != seqs(1).name) println("[Error] paired reads have different names: " + seqs(0).name + ", " + seqs(1).name)

    }

    adamVec.toArray   // return value
  }


  /**
    *  Finish the rest of computation after pair-end SW: pair-end alignments to SAM format 
    *  Used for batched processing. (pure Java execution)
    *  This function can be used for both (1) pure Java batched processing, and (2) JNI batched processing
    *  This function return ADAM format output
    *
    *  @param opt the input MemOptType object
    *  @param bns the BNTSeqType object
    *  @param pac the PAC array
    *  @param pes the pair-end statistics (output)
    *  @param groupSize the current read group (the size depends on the data written in Parquet format)
    *  @param id the current read id
    *  @param seqsPairs the input pair-end reads in this group (from Parquet/Avro data format)
    *  @param seqsTransPairs the byte arrays of the input pair-end sequences
    *  @param alnRegVecPairs the alignments of the input pair-end reads in this group
    *  @param samHeader the SAM header required to output SAM strings
    *  @param seqDict the sequences (chromosome) dictionary: used for ADAM format output
    *  @param readGroup the read group: used for ADAM format output
    *  @return an array of ADAM format object
    */
  private def memADAMPeGroupRest(opt: MemOptType, bns: BNTSeqType,  pac: Array[Byte], pes: Array[MemPeStat], groupSize: Int, id: Long,
                                seqsPairs: Array[Array[FASTQRecord]], seqsTransPairs: Array[Array[Array[Byte]]], alnRegVecPairs: Array[Array[Array[MemAlnRegType]]],
                                samHeader: SAMHeader, seqDict: SequenceDictionary, readGroup: RecordGroup): Array[AlignmentRecord] = {
    var k = 0
    var sum = 0
    var adamVec: Vector[AlignmentRecord] = scala.collection.immutable.Vector.empty 

    // Array index -
    // k: the group id
    // i: end id (left end or right end)
    // j: the alignment id
    while(k < groupSize) {

      var z: Array[Int] = new Array[Int](2)
      var subo: Int = 0
      var numSub: Int = 0
      var extraFlag: Int = 1
      var seqsTrans: Array[Array[Byte]] = new Array[Array[Byte]](2)
      var alnRegVec: Array[Array[MemAlnRegType]] = new Array[Array[MemAlnRegType]](2)
      var seqs: Array[FASTQRecord] = new Array[FASTQRecord](2)
      var noPairingFlag: Boolean = false

      seqs(0) = seqsPairs(k)(0)
      seqs(1) = seqsPairs(k)(1)
      seqsTrans(0) = seqsTransPairs(k)(0)
      seqsTrans(1) = seqsTransPairs(k)(1)
      alnRegVec(0) = alnRegVecPairs(k)(0)
      alnRegVec(1) = alnRegVecPairs(k)(1)      

      //println("id: " + (id + k))
      alnRegVec(0) = memMarkPrimarySe(opt, alnRegVec(0), (id + k)<<1|0)  // id -> id + k
      alnRegVec(1) = memMarkPrimarySe(opt, alnRegVec(1), (id + k)<<1|1)  // id -> id + k

      if((opt.flag & MEM_F_NOPAIRING) == 0) {
        var n = 0
        // pairing single-end hits
        var a0Size = 0
        var a1Size = 0
        if(alnRegVec(0) != null) a0Size = alnRegVec(0).size
        if(alnRegVec(1) != null) a1Size = alnRegVec(1).size

        if(alnRegVec(0) != null && alnRegVec(1) != null) {
          val retVal = memPair(opt, bns.l_pac, pes, alnRegVec, id + k)  // id -> id + k
          val ret = retVal._1
          subo = retVal._2
          numSub = retVal._3
          z = retVal._4

          if(ret > 0) {
            var scoreUn: Int = 0
            var isMulti: Array[Boolean] = new Array[Boolean](2)
            var qPe: Int = 0
            var qSe: Array[Int] = new Array[Int](2)

            var i = 0
            while(i < 2) {
              var j = 1
              var isBreak = false
              while(j < alnRegVec(i).size && !isBreak) {
                if(alnRegVec(i)(j).secondary < 0 && alnRegVec(i)(j).score >= opt.T) 
                  isBreak = true
                else 
                  j += 1
              }
                  
              if(j < alnRegVec(i).size) isMulti(i) = true
              else isMulti(i) = false

              i += 1
            }

            if(!isMulti(0) && !isMulti(1)) {  
              // compute mapQ for the best SE hit
              scoreUn = alnRegVec(0)(0).score + alnRegVec(1)(0).score - opt.penUnpaired
              if(subo < scoreUn) subo = scoreUn
              // Inline raw_mapq(ret - subo, opt.a)
              // #define raw_mapq(diff, a) ((int)(6.02 * (diff) / (a) + .499))
              qPe = (6.02 * (ret - subo) / opt.a + 0.499).toInt
            
              if(numSub > 0) qPe -= (4.343 * log(numSub + 1) + .499).toInt
              if(qPe < 0) qPe = 0
              if(qPe > 60) qPe = 60
  
              // the following assumes no split hits
              if(ret > scoreUn) { // paired alignment is preferred
                var tmpRegs = new Array[MemAlnRegType](2)
                tmpRegs(0) = alnRegVec(0)(z(0))
                tmpRegs(1) = alnRegVec(1)(z(1))

                var i = 0
                while(i < 2) {
                  if(tmpRegs(i).secondary >= 0) {
                    tmpRegs(i).sub = alnRegVec(i)(tmpRegs(i).secondary).score
                    tmpRegs(i).secondary = -1
                  }
  
                  qSe(i) = memApproxMapqSe(opt, tmpRegs(i))
                  i += 1
                }

                if(qSe(0) < qPe) {
                  if(qPe < qSe(0) + 40) qSe(0) = qPe
                  else qSe(0) = qSe(0) + 40
                }

                if(qSe(1) < qPe) {
                  if(qPe < qSe(1) + 40) qSe(1) = qPe
                  else qSe(1) = qSe(1) + 40
                }
              
                extraFlag |= 2
                           
                // cap at the tandem repeat score
                // Inline raw_mapq(tmpRegs(0).score - tmpRegs(0).csub, opt.a)
                var tmp = (6.02 * (tmpRegs(0).score - tmpRegs(0).csub) / opt.a + 0.499).toInt
                if(qSe(0) > tmp) qSe(0) = tmp
                // Inline raw_mapq(tmpRegs(1).score - tmpRegs(1).csub, opt.a)
                tmp = (6.02 * (tmpRegs(1).score - tmpRegs(1).csub) / opt.a + 0.499).toInt
                if(qSe(1) > tmp) qSe(1) = tmp
              }
              else { // the unpaired alignment is preferred
                z(0) = 0
                z(1) = 0
                qSe(0) = memApproxMapqSe(opt, alnRegVec(0)(0))
                qSe(1) = memApproxMapqSe(opt, alnRegVec(1)(0))
              }
            
              // write SAM
              val aln0 = memRegToAln(opt, bns, pac, seqs(0).seqLength, seqsTrans(0), alnRegVec(0)(z(0))) 
              aln0.mapq = qSe(0).toShort
              aln0.flag |= 0x40 | extraFlag
              val aln1 = memRegToAln(opt, bns, pac, seqs(1).seqLength, seqsTrans(1), alnRegVec(1)(z(1)))
              aln1.mapq = qSe(1).toShort
              aln1.flag |= 0x80 | extraFlag
  
              var alnList0 = new Array[MemAlnType](1)
              alnList0(0) = aln0
              adamVec = adamVec :+ memAlnToADAM(bns, seqs(0), seqsTrans(0), alnList0, 0, aln1, samHeader, seqDict, readGroup)
              var alnList1 = new Array[MemAlnType](1)
              alnList1(0) = aln1
              adamVec = adamVec :+ memAlnToADAM(bns, seqs(1), seqsTrans(1), alnList1, 0, aln0, samHeader, seqDict, readGroup)

              if(seqs(0).name != seqs(1).name) println("[Error] paired reads have different names: " + seqs(0).name + ", " + seqs(1).name)
            
            }
            else // else: goto no_pairing (TODO: in rare cases, the true hit may be long but with low score)
              noPairingFlag = true
          }
          else // else: goto no_pairing
            noPairingFlag = true
        }
        else // else: goto no_pairing 
          noPairingFlag = true 
      }
      else // else: goto no_pairing
        noPairingFlag = true

      if(noPairingFlag) { // no_pairing: start from here
        var alnVec: Array[MemAlnType] = new Array[MemAlnType](2)

        var i = 0
        while(i < 2) {
          if(alnRegVec(i) != null && alnRegVec(i).size > 0 && alnRegVec(i)(0).score >= opt.T) alnVec(i) = memRegToAln(opt, bns, pac, seqs(i).seqLength, seqsTrans(i), alnRegVec(i)(0)) 
          else alnVec(i) = memRegToAln(opt, bns, pac, seqs(i).seqLength, seqsTrans(i), null) 

          i += 1
        }

        // if the top hits from the two ends constitute a proper pair, flag it.
        if((opt.flag & MEM_F_NOPAIRING) == 0 && alnVec(0).rid == alnVec(1).rid && alnVec(0).rid >= 0) {
          // Inline mem_infer_dir
          var r1: Boolean = false
          var r2: Boolean = false
          if(alnRegVec(0)(0).rBeg >= bns.l_pac) r1 = true
          if(alnRegVec(1)(0).rBeg >= bns.l_pac) r2 = true

          var rBegLarger = alnRegVec(1)(0).rBeg
          // rBegLarger is the coordinate of read 2 on the read 1 strand
          if(r1 != r2) rBegLarger = (bns.l_pac << 1) - 1 - alnRegVec(1)(0).rBeg
          var dist: Int = (alnRegVec(0)(0).rBeg - rBegLarger).toInt
          if(rBegLarger > alnRegVec(0)(0).rBeg) dist = (rBegLarger - alnRegVec(0)(0).rBeg).toInt

          var cond1 = 1
          if(r1 == r2) cond1 = 0
          var cond2 = 3
          if(rBegLarger > alnRegVec(0)(0).rBeg) cond2 = 0

          var d = cond1 ^ cond2
       
          if(pes(d).failed == 0 && dist >= pes(d).low && dist <= pes(d).high) extraFlag |= 2 
        }

        adamVec = adamVec ++ memRegToADAMSe(opt, bns, pac, seqs(0), seqsTrans(0), alnRegVec(0), 0x41 | extraFlag, alnVec(1), samHeader, seqDict, readGroup)
        adamVec = adamVec ++ memRegToADAMSe(opt, bns, pac, seqs(1), seqsTrans(1), alnRegVec(1), 0x81 | extraFlag, alnVec(0), samHeader, seqDict, readGroup)

        if(seqs(0).name != seqs(1).name) println("[Error] paired reads have different names: " + seqs(0).name + ", " + seqs(1).name)
      }

      k += 1
    }

    adamVec.toArray
  }


  /**
    *  memADAMPeGroup: Pair-end alignments to SAM format 
    *  Used for batched processing. (pure Java version)
    *  Generate ADAM format output
    *
    *  @param opt the input MemOptType object
    *  @param bns the BNTSeqType object
    *  @param pac the PAC array
    *  @param pes the pair-end statistics (output)
    *  @param groupSize the current read group (the size depends on the data written in Parquet format)
    *  @param id the current read id
    *  @param seqsPairsIn the input pair-end reads in this group (from Parquet/Avro data format)
    *  @param alnRegVecPairs the alignments of the input pair-end reads in this group
    *  @param samHeader the SAM header required to output SAM strings
    *  @param seqDict the sequences (chromosome) dictionary: used for ADAM format output
    *  @param readGroup the read group: used for ADAM format output
    *  @return an array of ADAM format object
    */
  def memADAMPeGroup(opt: MemOptType, bns: BNTSeqType, pac: Array[Byte], pes: Array[MemPeStat], groupSize: Int, id: Long, seqsPairsIn: Array[PairEndFASTQRecord], 
                     alnRegVecPairs: Array[Array[Array[MemAlnRegType]]], samHeader: SAMHeader, seqDict: SequenceDictionary, readGroup: RecordGroup): Array[AlignmentRecord] = {

    var seqsPairs = initSingleEndPairs(seqsPairsIn)
    var seqsTransPairs = initSeqsTransPairs(seqsPairs, groupSize)
    val prepRet = memSamPeGroupPrepare(opt, bns, pac, pes, groupSize, alnRegVecPairs, seqsPairs)
    val n = memSamPeGroupMateSW(opt, bns.l_pac, pes, groupSize, seqsPairs, seqsTransPairs, prepRet._1, alnRegVecPairs, prepRet._2)
    memADAMPeGroupRest(opt, bns,  pac, pes, groupSize, id, seqsPairs, seqsTransPairs, alnRegVecPairs, samHeader, seqDict, readGroup)
  }


  /**
    *  memSamPeGroupJNI: Pair-end alignments to SAM format 
    *  Used for JNI batched processing. (native C library)
    *
    *  @param opt the input MemOptType object
    *  @param bns the BNTSeqType object
    *  @param pac the PAC array
    *  @param pes the pair-end statistics (output)
    *  @param groupSize the current read group (the size depends on the data written in Parquet format)
    *  @param id the current read id
    *  @param seqsPairsIn the input pair-end reads in this group (from Parquet/Avro data format)
    *  @param alnRegVecPairs the alignments of the input pair-end reads in this group
    *  @param samHeader the SAM header required to output SAM strings
    *  @param seqDict the sequences (chromosome) dictionary: used for ADAM format output
    *  @param readGroup the read group: used for ADAM format output
    *  @return an array of ADAM format object
    */
  def memADAMPeGroupJNI(opt: MemOptType, bns: BNTSeqType, pac: Array[Byte], pes: Array[MemPeStat], groupSize: Int, id: Long, 
                        seqsPairsIn: Array[PairEndFASTQRecord], alnRegVecPairs: Array[Array[Array[MemAlnRegType]]],
                        samHeader: SAMHeader, seqDict: SequenceDictionary, readGroup: RecordGroup): Array[AlignmentRecord] = {

    // prepare seqsPairs array
    var seqsPairs = initSingleEndPairs(seqsPairsIn)
    // prepare seqsTransPairs array
    var seqsTransPairs = initSeqsTransPairs(seqsPairs, groupSize)
    // use memSamPeGroupJNIPrepare() to prepare data before calling JNI function
    val ret = memSamPeGroupJNIPrepare(opt, bns,  pac, pes, groupSize, seqsPairs, seqsTransPairs, alnRegVecPairs)
    
    // call JNI function to use native library
    val mateSWArray = ret._1
    val seqsSWArray = ret._2
    val refSWArray = ret._3
    val refSWArraySize = ret._4
    val jni = new MateSWJNI
    val retMateSWArray = jni.mateSWJNI(opt, bns.l_pac, pes, groupSize, seqsSWArray, mateSWArray, refSWArray, refSWArraySize)

    // transform the output array obtained from JNI
    val alnRegVecPairsJNI = mateSWArrayToAlnRegPairArray(groupSize, retMateSWArray)

    // run the rest of alingment -> SAM function (Smith-Waterman algorithm to generate CIGAR string)
    memADAMPeGroupRest(opt, bns,  pac, pes, groupSize, id, seqsPairs, seqsTransPairs, alnRegVecPairsJNI, samHeader, seqDict, readGroup)
  }

}


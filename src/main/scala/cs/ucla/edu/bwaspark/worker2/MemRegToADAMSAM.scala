package cs.ucla.edu.bwaspark.worker2

import scala.util.control.Breaks._
import scala.List
import scala.collection.mutable.MutableList
import scala.math.log
import scala.math.abs

import cs.ucla.edu.bwaspark.datatype._
import cs.ucla.edu.bwaspark.util.BNTSeqUtil._
import cs.ucla.edu.bwaspark.util.SWUtil._

object MemRegToADAMSAM {
  val MEM_F_ALL = 0x8
  val MEM_F_NO_MULTI = 0x10
  val MEM_MAPQ_COEF = 30.0

  /**
    *  Transform the alignment registers to SAM format
    *  
    *  @param opt the input MemOptType object
    *  @param bns the input BNTSeqType object
    *  @param pac the PAC array
    *  @param seq the read (NOTE: currently we use Array[Byte] first. may need to be changed back to Array[Char]!!!)
    *  @param regs the alignment registers to be transformed
    *  @param extraFlag
    *  @param alns 
    */
  def memRegToSAMSe(opt: MemOptType, bns: BNTSeqType, pac: Array[Byte], seq: Array[Byte], regs: MutableList[MemAlnRegType], extraFlag: Int, alnsIn: MutableList[MemAlnType]) {
    var alns: MutableList[MemAlnType] = new MutableList[MemAlnType]

    // NOTE: set opt.flag manually here!!! This should be modified from the logger!!!
    opt.flag = 24
/*   
    println("[opt object] flag: " + opt.flag + " T: " + opt.T + " minSeedLen: " + opt.minSeedLen + " a: " + opt.a + " b: " + opt.b + " mapQCoefLen: " + opt.mapQCoefLen + " mapQCoefFac: " + opt.mapQCoefFac)

    var j = 0
    regs.foreach(r => {
      print("Reg " + j + "(")
      print(r.rBeg + ", " + r.rEnd + ", " + r.qBeg + ", " + r.qEnd + ", " + r.score + ", " + r.trueScore + ", ")
      println(r.sub + ", "  + r.csub + ", " + r.subNum + ", " + r.width + ", " + r.seedCov + ", " + r.secondary + ")")
      j += 1
      } )
*/
    for(i <- 0 to (regs.length - 1)) {
      if(regs(i).score >= opt.T) {
        if(regs(i).secondary < 0 || ((opt.flag & MEM_F_ALL) > 0)) {
          if(regs(i).secondary < 0 || regs(i).score >= regs(regs(i).secondary).score * 0.5) {
            // debugging
            //print("i=" + i + " ")
            var aln = memRegToAln(opt, bns, pac, 101, seq, regs(i))   // NOTE: current data structure has not been obtained from RDD. We assume the length to be 101 here
            alns += aln
            aln.flag |= extraFlag   // flag secondary
            if(regs(i).secondary >= 0) aln.sub = -1   // don't output sub-optimal score
            if(i > 0 && regs(i).secondary < 0)   // if supplementary
              if((opt.flag & MEM_F_NO_MULTI) > 0) aln.flag |= 0x10000
              else aln.flag |= 0x800

            if(i > 0 && aln.mapq > alns(0).mapq) aln.mapq = alns(0).mapq            
          }
        }
      }
    }

    // no alignments good enough; then write an unaligned record
    if(alns.length == 0) { 
      var aln = memRegToAln(opt, bns, pac, 101, seq, null)
      aln.flag |= extraFlag
      //memAlnToSAM
    }
    else {
      //alns.foreach(memAlnToSAM)
    }
  }


  //basic hit->SAM conversion
  /**
    *
    @param l1
    @param l2
    @param score
    @param a
    @param q
    @param r
    @param w, output
    */

   //def inferBw( l1: Int, l2: Int, score: Int, a: Int, q: Int, r: Int) : Int = {
   private def inferBw( l1: Int, l2: Int, score: Int, a: Int, q: Int, r: Int) : Int = {
     var w: Int = 0;
     if(l1 == l2 && (((l1 * a) - score) < ((q + r - a) << 1))) {
     }
     else {
       var wDouble: Double = 0
       wDouble = (((if( l1 < l2 ) l1 else l2) * a - score - q).toDouble / r.toDouble + 2.toDouble)
       var absDifference: Int = if( l1 < l2 ) ( l2 - l1) else ( l1 - l2)
       if( wDouble < absDifference ) w = absDifference
       else w = wDouble.toInt
       //w is the returned Integer
     }
     w
   }

   /*
   get_rLen actually this function is barely called in the current flow
   @param n_cigar
   @param *cigar 
   @param l output


   */
  // this one is not tested because it is never been used in 20 reads dataset

  private def getRlen(cigar: List[Int]) : Int = {

    var l: Int = 0
    val nCigar = cigar.length
    for(k <- 0 to nCigar - 1) {
      var op: Int = cigar(k) & 0xf
      if( op == 0 || op == 2) l += cigar(k) >>> 4

    }
    l
  }
	

  // wrapper implementation only for now
  /**
    *  Transform the alignment registers to alignment type
    *
    *  @param opt the input MemOptType object
    *  @param bns the input BNTSeqType object
    *  @param pac the PAC array
    *  @param seqLen the length of the input sequence
    *  @param seq the input sequence (NOTE: currently we use Array[Byte] first. may need to be changed back to Array[Char]!!!)
    *  @param reg the input alignment register
    */
  private def memRegToAln(opt: MemOptType, bns: BNTSeqType, pac: Array[Byte], seqLen: Int, seq: Array[Byte], reg: MemAlnRegType): MemAlnType = {
    val aln = new MemAlnType

    if(reg == null || reg.rBeg < 0 || reg.rEnd < 0) {
      aln.rid = -1
      aln.pos = -1
      aln.flag |= 0x4
      aln
    }
    else {
      val qb = reg.qBeg
      val qe = reg.qEnd
      val rb = reg.rBeg
      val re = reg.rEnd

      if(reg.secondary < 0) 
        aln.mapq = memApproxMapqSe(opt, reg).toShort
      else
        aln.mapq = 0

      // secondary alignment
      if(reg.secondary >= 0) aln.flag |= 0x100 

      val ret = bwaFixXref2(opt.mat, opt.oDel, opt.eDel, opt.oIns, opt.eIns, opt.w, bns, pac, seq, reg.qBeg, reg.qEnd, reg.rBeg, reg.rEnd)
      val iden = ret._5
      if(iden < 0) {
        println("[Error] If you see this message, please let the developer know. Abort. Sorry.")
        assert(false, "bwaFixXref2() problem")
      }

      var tmp = inferBw(qe - qb, (re - rb).toInt, reg.trueScore, opt.a, opt.oDel, opt.eDel)
      var w2 = inferBw(qe - qb, (re - rb).toInt, reg.trueScore, opt.a, opt.oIns, opt.eIns)
      if(w2 < tmp) w2 = tmp
      if(w2 > opt.w) {
        if(w2 > reg.width) w2 = reg.width
      }
      //else w2 = opt.w  // TODO: check if we need this line on long reads. On 1-800bp reads, it does not matter and it should be. (In original C implementation)

      var i = 0
      aln.cigar = null
      var score = 0
      var lastScore = -(1 << 30)

      breakable {
        do {
          // make a copy to pass into bwaGenCigar2
          // if there is performance issue later, we may modify the underlying implementation
          var query: Array[Byte] = new Array[Byte](qe - qb)
          for(i <- 0 to (qe - qb - 1)) query(i) = seq(qb + i)
 
          var ret = bwaGenCigar2(opt.mat, opt.oDel, opt.eDel, opt.oIns, opt.eIns, w2, bns.l_pac, pac, qe - qb, query, rb, re)
          score = ret._1
          aln.nCigar = ret._2
          aln.NM = ret._3
          aln.cigar = ret._4

          if(score == lastScore) break
          lastScore = score
          w2 <<= 1

          i += 1
        } while(i < 3 && score < reg.trueScore - opt.a)

      }

      var pos: Long = 0
      var isRev: Int = 0
 
      if(rb < bns.l_pac) {
        val ret = bnsDepos(bns, rb)
        pos = ret._1
        isRev = ret._2
      }
      else {
        val ret = bnsDepos(bns, re - 1)
        pos = ret._1
        isRev = ret._2
      }

      aln.isRev = isRev.toByte

      // squeeze out leading or trailing deletions
      if(aln.nCigar > 0) {
        if(aln.cigar.cigarSegs(0).op == 2) {
          pos += aln.cigar.cigarSegs(0).len
          aln.cigar.cigarSegs = aln.cigar.cigarSegs.drop(1)
          aln.nCigar -= 1
        }
        else if(aln.cigar.cigarSegs(aln.nCigar - 1).op == 2) {            
          aln.cigar.cigarSegs = aln.cigar.cigarSegs.dropRight(1)
          aln.nCigar -= 1
        }
      }

      // add clipping to CIGAR
      if(qb != 0 || qe != seqLen) {
        var clip5 = 0
        var clip3 = 0

        if(isRev > 0) clip5 = seqLen - qe
        else clip5 = qb

        if(isRev > 0) clip3 = qb
        else clip3 = seqLen - qe

        if(clip5 > 0) {
          val cigarSeg = new CigarSegType
          cigarSeg.op = 3
          cigarSeg.len = clip5
          aln.cigar.cigarSegs.+=:(cigarSeg)
          aln.nCigar += 1
        }
        
        if(clip3 > 0) {
          val cigarSeg = new CigarSegType
          cigarSeg.op = 3
          cigarSeg.len = clip3
          aln.cigar.cigarSegs += cigarSeg
          aln.nCigar += 1
        }
      }

      aln.rid = bnsPosToRid(bns, pos)
      aln.pos = pos - bns.anns(aln.rid).offset
      aln.score = reg.score
      if(reg.sub > reg.csub) aln.sub = reg.sub
      else aln.sub = reg.csub
      
      aln
    }
  }

  /**
    *  Calculate the approximate mapq value
    *
    *  @param opt the input MemOptType object
    *  @param reg the input alignment register
    */
  private def memApproxMapqSe(opt: MemOptType, reg: MemAlnRegType): Int = {
    var sub = 0
    
    if(reg.sub > 0) sub = reg.sub
    else sub = opt.minSeedLen * opt.a

    if(reg.csub > sub) sub = reg.csub
   
    if(sub >= reg.score) 0
    else {
      var len = 0
      var mapq = 0
      
      if(reg.qEnd - reg.qBeg > reg.rEnd - reg.rBeg) len = reg.qEnd - reg.qBeg
      else len = (reg.rEnd - reg.rBeg).toInt
      
      val identity = 1.0 - (len * opt.a - reg.score).toDouble / (opt.a + opt.b) / len
      if(reg.score == 0) mapq = 0
      else if(opt.mapQCoefLen > 0) {
        var tmp: Double = 1.0
        if(len > opt.mapQCoefLen) tmp = opt.mapQCoefFac / log(len)
        tmp *= identity * identity
        mapq = (6.02 * (reg.score - sub) / opt.a * tmp * tmp + 0.499).toInt
      }
      else {
        mapq = (MEM_MAPQ_COEF * (1.0 - sub.toDouble / reg.score) * log(reg.seedCov) + .499).toInt
        if(identity < 0.95) mapq = (mapq * identity * identity + .499).toInt
      }
   
      if(reg.subNum > 0) mapq -= (4.343 * log(reg.subNum + 1) + .499).toInt
      if(mapq > 60) mapq = 60
      if(mapq < 0) mapq = 0

      mapq
    }

  }  

  // wrapper implementation only for now
  private def bwaFixXref2(mat: Array[Byte], oDel: Int, eDel: Int, oIns: Int, eIns: Int, w: Int, bns: BNTSeqType, 
    pac: Array[Byte], query: Array[Byte], qBeg: Int, qEnd: Int, rBeg: Long, rEnd: Long): (Int, Int, Long, Long, Int) = {
    var retArray = new Array[Int](5)

    // cross the for-rev boundary; actually with BWA-MEM, we should never come to here
    if(rBeg < bns.l_pac && rEnd > bns.l_pac) {
      println("[ERROR] In BWA-mem, we should never come to here")
      (-1, -1, -1, -1, -1)  // unable to fix
    }
    else {
      val ret = bnsDepos(bns, (rBeg + rEnd) >> 1)  // coordinate of the middle point on the forward strand
      val fm = ret._1
      val isRev = ret._2
      val ra = bns.anns(bnsPosToRid(bns, fm))  // annotation of chr corresponding to the middle point
      var cb = ra.offset
      if(isRev > 0) cb = (bns.l_pac << 1) - (ra.offset + ra.len)
      var ce = cb + ra.len   // chr end
      var qBegRet = qBeg
      var qEndRet = qEnd
      var rBegRet = rBeg
      var rEndRet = rEnd

      // fix is needed
      if(cb > rBeg || ce < rEnd) {
        if(cb < rBeg) cb = rBeg
        if(ce > rEnd) ce = rEnd

        var queryArr: Array[Byte] = new Array[Byte](qEnd - qBeg)
        // make a copy to pass into bwaGenCigar2 
        // if there is performance issue later, we may modify the underlying implementation
        for(i <- 0 to (qEnd - qBeg - 1)) queryArr(i) = query(qBeg + i)

        val ret = bwaGenCigar2(mat, oDel, eDel, oIns, eIns, w, bns.l_pac, pac, qEnd - qBeg, queryArr, rBeg, rEnd)
        val numCigar = ret._2
        val cigar = ret._4

        var x = rBeg
        var y = qBeg

        breakable {
          for(i <- 0 to (numCigar - 1)) {
            val op = cigar.cigarSegs(i).op
            val len = cigar.cigarSegs(i).len

            if(op == 0) {
              if(x <= cb && cb < x + len) {
                qBegRet = (y + (cb - x)).toInt
                rBegRet = cb
              }
              
              if(x < ce && ce <= x + len) {
                qEndRet = (y + (ce - x)).toInt
                rEndRet = ce
                break
              }
              else {
                x += len
                y += len
              }
            }
            else if(op == 1) {
              y += len
            } 
            else if(op == 2) {
              if(x <= cb && cb < x + len) {
                qBegRet = y
                rBegRet = x + len
              }
              
              if(x < ce && ce <= x + len) {
                qEndRet = y
                rEndRet = x
              }
              else x += len
            }
            else {
              println("[Error] Should not be here!!!")
              assert(false, "in bwaFixXref2()")
            }
          }
        }
        // NOTE!!!: Need to be implemented!!! temporarily skip this for loop
      }
    
      var iden = 0
      if(qBegRet == qEndRet || rBegRet == rEndRet) iden = -2
      (qBegRet, qEndRet, rBegRet, rEndRet, iden)
    }

  }

  private def bwaGenCigar2(mat: Array[Byte], oDel: Int, eDel: Int, oIns: Int, eIns: Int, w: Int, pacLen: Long, pac: Array[Byte], 
    queryLen: Int, query_i: Array[Byte], rBeg: Long, rEnd: Long): (Int, Int, Int, CigarType) = {

    var numCigar = 0
    var NM = -1
    var score = 0
    var cigar = new CigarType

    // reject if negative length or bridging the forward and reverse strand
    if(queryLen <= 0 || rBeg >= rEnd || (rBeg < pacLen && rEnd > pacLen)) (0, 0, 0, null)
    else {
      val ret = bnsGetSeq(pacLen, pac, rBeg, rEnd)
      var rseq = ret._1
      val rlen = ret._2

      // possible if out of range
      if(rEnd - rBeg != rlen) (0, 0, 0, null)
      else {
        var query = query_i

        // then reverse both query and rseq; this is to ensure indels to be placed at the leftmost position
        if(rBeg >= pacLen) {
          for(i <- 0 to ((queryLen >> 1) - 1)) {
            var tmp = query(i)
            query(i) = query(queryLen - 1 - i)
            query(queryLen - 1 - i) = tmp
          }
            
          for(i <- 0 to ((rlen >> 1) - 1).toInt) {
            var tmp = rseq(i)
            rseq(i) = rseq((rlen - 1 - i).toInt)
            rseq((rlen - 1 - i).toInt) = tmp
          }
        }        
        // no gap; no need to do DP
        if(queryLen == (rEnd - rBeg) && w == 0) {
          // FIXME: due to an issue in mem_reg2aln(), we never come to this block. This does not affect accuracy, but it hurts performance. (in original C implementation)
          //println("ENTER!!!")
          var cigarSeg = new CigarSegType
          cigarSeg.len = queryLen 
          cigarSeg.op = 0
          numCigar = 1
          cigar.cigarSegs += cigarSeg

          for(i <- 0 to (queryLen - 1)) 
            score += mat(rseq(i) * 5 + query(i))
        }
        else {
          val maxIns = ((((queryLen + 1) >> 1) * mat(0) - oIns).toDouble / eIns + 1.0).toInt
          val maxDel = ((((queryLen + 1) >> 1) * mat(0) - oDel).toDouble / eDel + 1.0).toInt
          var maxGap = maxIns
          if(maxIns < maxDel) maxGap = maxDel

          var width = (maxGap + abs((rlen - queryLen) + 1)) >> 1
          if(width > w) width = w
          val minWidth = abs(rlen - queryLen) + 3
          if(width < minWidth) width = minWidth
          // NW alignment
          val ret = SWGlobal(queryLen, query, rlen.toInt, rseq, 5, mat, oDel, eDel, oIns, eIns, width.toInt, numCigar, cigar.cigarSegs)
          score = ret._1
          numCigar = ret._2
        }
       
        // compute NM and MD
        // str.l = str.m = *n_cigar * 4; str.s = (char*)cigar; // append MD to CIGAR   
        var int2base = new Array[Char](5)
        var x = 0
        var y = 0
        var u = 0
        var n_mm = 0
        var nGap = 0

        if(rBeg < pacLen) int2base = Array('A', 'C', 'G', 'T', 'N')
        else int2base = Array('T', 'G', 'C', 'A', 'N')        

        println("numCigar: " + numCigar)

        for(k <- 0 to (numCigar - 1)) {
          val op = cigar.cigarSegs(k).op
          val len = cigar.cigarSegs(k).len
        
          println("op " + op + ", len " + len)
  
          // match
          if(op == 0) {
            for(i <- 0 to (len - 1)) {
              if(query(x + i) != rseq(y + i)) {
                cigar.cigarStr += u.toString
                cigar.cigarStr += int2base(rseq(y + i))
                n_mm += 1
                u = 0
              }
              else u += 1
            }

            x += len
            y += len
          }
          // deletion
          else if(op == 2) {
            // don't do the following if D is the first or the last CIGAR
            if(k > 0 && k < numCigar - 1) {
              cigar.cigarStr += u.toString
              cigar.cigarStr += '^'
              
              for(i <- 0 to (len - 1)) cigar.cigarStr += int2base(rseq(y + i))
              
              u = 0
              nGap += len
            }

            y += len
          }
          // insertion
          else if(op == 1) {
            x += len
            nGap += len
          }
        }
        
        cigar.cigarStr += u.toString
        var NM = n_mm + nGap

        // reverse back query 
        // This is done in original C implementation. However, in the current implementation, the query is a copy
        // from the original array. Therefore, we do not need to reverse this copy back
        //if(rBeg >= pacLen) 
          //for(i <- 0 to ((queryLen >> 1) - 1)) {
            //var tmp = query(i)
            //query(i) = query(queryLen - 1 - i)
            //query(queryLen - 1 - i) = tmp
          //}
        
        println(cigar.cigarStr)

        (score, numCigar, NM, cigar)
      }
    }

  }
}


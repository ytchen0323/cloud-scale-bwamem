package cs.ucla.edu.bwaspark.util

import scala.util.control.Breaks._
import scala.math.abs
import scala.collection.mutable.MutableList

import cs.ucla.edu.bwaspark.datatype.CigarSegType

object SWUtil {
  val MINUS_INF = -0x40000000

  /**
    *  EHType: use for Smith Waterman Extension
    */
  class EHType(e_i: Int, h_i: Int) {
    var e: Int = e_i
    var h: Int = h_i
  }


  /**
    *  Smith-Waterman Extension
    *  The key function in both left and right extension
    *
    *  @param qLen the query length
    *  @param query the query (read)
    *  @param tLen the target (reference) length retrieved
    *  @param target the target (reference) retrieved
    *  @param m the (mat) array size in one dimension 
    *  @param mat the mat array
    *  @param oDel oDel in the input MemOptType object
    *  @param eDel eDel in the input MemOptType object
    *  @param oIns oIns in the input MemOptType object
    *  @param eIns eIns in the input MemOptType object
    *  @param w_i the input weight
    *  @param endBonus endBonus in the input MemOptType object
    *  @param zdrop zdrop in the input MemOptType object
    *  @param h0 initial S-W score
    */  
  def SWExtend(
    qLen: Int, query: Array[Byte], tLen: Int, target: Array[Byte], m: Int, mat: Array[Byte],
    oDel: Int, eDel: Int, oIns: Int, eIns: Int, w_i: Int, endBonus: Int, zdrop: Int, h0: Int): Array[Int] =  
  {
    var retArray: Array[Int] = new Array[Int](6)
    var eh: Array[EHType] = new Array[EHType](qLen + 1) // score array
    var qp: Array[Byte] = new Array[Byte](qLen * m) // query profile
    var oeDel = oDel + eDel
    var oeIns = oIns + eIns
    var i = 0 
    var j = 0 
    var k = 0
    var w = w_i

    while(i < (qLen + 1)) {
      eh(i) = new EHType(0, 0)
      i += 1
    }

    // generate the query profile
    i = 0
    k = 0
    while(k < m) {
      val p = k * m
      
      j = 0
      while(j < qLen) {
        qp(i) = mat(p + query(j))
        i += 1
        j += 1
      }

      k += 1
    }
    
    // fill the first row
    eh(0).h = h0
    if(h0 > oeIns) eh(1).h = h0 - oeIns
    else eh(1).h = 0
    j = 2
    while(j <= qLen && eh(j-1).h > eIns) {
      eh(j).h = eh(j-1).h - eIns
      j += 1
    }

    // adjust $w if it is too large
    k = m * m
    var max = 0
    max = mat.max // get the max score
    var maxIns = ((qLen * max + endBonus - oIns).toDouble / eIns + 1.0).toInt
    if(maxIns < 1) maxIns = 1
    var maxDel = ((qLen * max + endBonus - oDel).toDouble / eDel + 1.0).toInt
    if(maxDel < 1) maxDel = 1
    if(w > maxDel) w = maxDel  // TODO: is this necessary? (in original C implementation)

    // DP loop
    max = h0
    var max_i = -1
    var max_j = -1
    var max_ie = -1
    var gscore = -1
    var max_off = 0
    var beg = 0
    var end = qLen

    //breakable {
    var isBreak = false
    i = 0
    while(i < tLen && !isBreak) {
      var t = 0
      var f = 0
      var h1 = 0
      var m = 0
      var mj = -1
      var qPtr = target(i) * qLen
      // compute the first column
      h1 = h0 - (oDel + eDel * (i + 1))
      if(h1 < 0) h1 = 0
      // apply the band and the constraint (if provided)
      if (beg < i - w) beg = i - w
      if (end > i + w + 1) end = i + w + 1
      if (end > qLen) end = qLen

      j = beg
      while(j < end) {
        // At the beginning of the loop: eh[j] = { H(i-1,j-1), E(i,j) }, f = F(i,j) and h1 = H(i,j-1)
        // Similar to SSE2-SW, cells are computed in the following order:
        //   H(i,j)   = max{H(i-1,j-1)+S(i,j), E(i,j), F(i,j)}
        //   E(i+1,j) = max{H(i,j)-gapo, E(i,j)} - gape
        //   F(i,j+1) = max{H(i,j)-gapo, F(i,j)} - gape
        var h = eh(j).h
        var e = eh(j).e   // get H(i-1,j-1) and E(i-1,j)
        eh(j).h = h1
        h += qp(qPtr + j)
        if(h < e) h = e
        if(h < f) h = f 
        h1 = h            // save H(i,j) to h1 for the next column
        if(m < h) { 
          mj = j          // record the position where max score is achieved
          m = h           // m is stored at eh[mj+1]
        }
        t = h - oeDel
        if(t < 0) t = 0
        e -= eDel
        if(e < t) e = t   // computed E(i+1,j)
        eh(j).e = e       // save E(i+1,j) for the next row
        t = h - oeIns
        if(t < 0) t = 0
        f -= eIns
        if(f < t) f = t
        j += 1
      }
      
      eh(end).h = h1
      eh(end).e = 0
      // end == j after the previous loop
      if(end == qLen) {
        if(gscore < h1) {
          max_ie = i
          gscore = h1
        }
      }

      if(m == 0) 
        isBreak = true
      else {
        if(m > max) {
          max = m
          max_i = i
          max_j = mj

          if(max_off < abs(mj - i)) max_off = abs(mj - i)
        }
        else if(zdrop > 0) {
          if((i - max_i) > (mj - max_j)) 
            if(max - m - ((i - max_i) - (mj - max_j)) * eDel > zdrop) isBreak = true
          else
            if(max - m - ((mj - max_j) - (i - max_i)) * eIns > zdrop) isBreak = true
        }
        
        // update beg and end for the next round
        if(!isBreak) {
          j = mj
          while(j >= beg && eh(j).h > 0) {
            j -= 1
          }
          beg = j + 1

          j = mj + 2
          while(j <= end && eh(j).h > 0) {
            j += 1
          }
          end = j
        }
      }

      i += 1
    }

    retArray(0) = max
    retArray(1) = max_j + 1
    retArray(2) = max_i + 1
    retArray(3) = max_ie + 1
    retArray(4) = gscore
    retArray(5) = max_off
    
    retArray
  }

  
  def SWGlobal(
    qLen: Int, query: Array[Byte], tLen: Int, target: Array[Byte], m: Int, mat: Array[Byte],
    oDel: Int, eDel: Int, oIns: Int, eIns: Int, w: Int, numCigar: Int, cigarSegs: MutableList[CigarSegType]): (Int, Int) = {

    var eh: Array[EHType] = new Array[EHType](qLen + 1) // score array
    var qp: Array[Byte] = new Array[Byte](qLen * m) // query profile
    var oeDel = oDel + eDel
    var oeIns = oIns + eIns
    var i = 0
    var k = 0
    var nCol = 0

    // maximum #columns of the backtrack matrix
    if(qLen < (2 * w + 1)) nCol = qLen 
    else nCol = 2 * w + 1
    var z: Array[Byte] = new Array[Byte](nCol * tLen)

    for(i <- 0 until (qLen + 1))
      eh(i) = new EHType(0, 0)


    // generate the query profile
    i = 0
    for(k <- 0 to (m - 1)) {
      val p = k * m

      for(j <- 0 to (qLen - 1)) {
        qp(i) = mat(p + query(j))
        i += 1
      }
    }

    // fill the first row
    eh(0).h = 0
    eh(0).e = MINUS_INF

    var j = 1
    while(j <= qLen && j <= w) {
      eh(j).h = -(oIns + eIns * j)
      eh(j).e = MINUS_INF
      j += 1
    }
    // everything is -inf outside the band
    while(j <= qLen) {
      eh(j).h = MINUS_INF
      eh(j).e = MINUS_INF
      j += 1
    }

    // DP loop
    for(i <- 0 to (tLen - 1)) {
      var f = MINUS_INF
      var qPtr = target(i) * qLen
      var zPtr = i * nCol
      var beg = 0
      var end = qLen
      var h1 = MINUS_INF

      if(i > w) beg = i - w
      if(i + w + 1 < qLen) end = i + w + 1  // only loop through [beg,end) of the query sequence
      if(beg == 0) h1 = -(oDel + eDel * (i + 1))

      for(j <- beg to (end - 1)) {
        // At the beginning of the loop: eh[j] = { H(i-1,j-1), E(i,j) }, f = F(i,j) and h1 = H(i,j-1)
        // Cells are computed in the following order:
        //   M(i,j)   = H(i-1,j-1) + S(i,j)
        //   H(i,j)   = max{M(i,j), E(i,j), F(i,j)}
        //   E(i+1,j) = max{M(i,j)-gapo, E(i,j)} - gape
        //   F(i,j+1) = max{M(i,j)-gapo, F(i,j)} - gape
        // We have to separate M(i,j); otherwise the direction may not be recorded correctly.
        // However, a CIGAR like "10M3I3D10M" allowed by local() and extend() is disallowed by global().
        // Such a CIGAR may occur, in theory, if mismatch_penalty > 2*gap_ext_penalty + 2*gap_open_penalty/k.
        // In practice, this should happen very rarely given a reasonable scoring system.
        var m = eh(j).h
        var e = eh(j).e
        eh(j).h = h1
        m += qp(qPtr + j)
        //var d: Byte = 1
        var d = 1  // temporarily use Int here
        if(m >= e) d = 0
        var h = e
        if(m >= e) h = m
        if(h < f) d = 2
        if(h < f) h = f
        h1 = h
        var t = m - oeDel
        e -= eDel
        if(e > t) d |= (1 << 2)
        else d |= 0
        if(e < t) e = t
        eh(j).e = e
        t = m - oeIns
        if(f > t) d |= (2 << 4)  // if we want to halve the memory, use one bit only, instead of two (in original C implementation)
        else d |= 0
        if(f < t) f = t
        z(zPtr + j - beg) = d.toByte  // z[i,j] keeps h for the current cell and e/f for the next cell
      }

      eh(end).h = h1
      eh(end).e = MINUS_INF
    }
    
    val score = eh(qLen).h
    
    // backtrack
    var numCigarTmp = numCigar
    var which = 0
    // (i,k) points to the last cell
    i = tLen - 1
    if(i + w + 1 < qLen) k = i + w
    else k = qLen - 1

    while(i >= 0 && k >= 0) {
      if(i > w) which = z(i * nCol + (k - (i - w))) >>> (which << 1) & 3
      else which = z(i * nCol + k) >>> (which << 1) & 3

      if(which == 0) { 
        numCigarTmp = pushCigar(numCigarTmp, cigarSegs, 0, 1) 
        i -= 1
        k -= 1
      }
      else if(which == 1) {
        numCigarTmp = pushCigar(numCigarTmp, cigarSegs, 2, 1)
        i -= 1
      }
      else {
        numCigarTmp = pushCigar(numCigarTmp, cigarSegs, 1, 1)
        k -= 1
      }
    }

    if(i >= 0) numCigarTmp = pushCigar(numCigarTmp, cigarSegs, 2, i + 1)
    if(k >= 0) numCigarTmp = pushCigar(numCigarTmp, cigarSegs, 1, k + 1)

    for(i <- 0 to ((numCigarTmp>>1) - 1)) {
      var tmp = cigarSegs(i)
      cigarSegs(i) = cigarSegs(numCigarTmp - 1 - i)
      cigarSegs(numCigarTmp - 1 - i) = tmp
    }

    (score, numCigarTmp)
  }
    
  
  private def pushCigar(numCigar: Int, cigarSegs: MutableList[CigarSegType], op: Byte, len: Int): Int = {
    var numCigarTmp = numCigar

    if(numCigarTmp == 0 || op != cigarSegs(numCigarTmp - 1).op) { 
      var cigarSeg = new CigarSegType
      cigarSeg.len = len
      cigarSeg.op = op
      cigarSegs += cigarSeg
      numCigarTmp += 1
    }
    else cigarSegs(numCigarTmp - 1).len += len

    numCigarTmp
  }

}

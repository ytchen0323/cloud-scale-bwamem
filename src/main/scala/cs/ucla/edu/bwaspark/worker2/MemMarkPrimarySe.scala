package cs.ucla.edu.bwaspark.worker2

import scala.List
import scala.math.abs

import cs.ucla.edu.bwaspark.datatype._

//MemMarkPrimarySe
object MemMarkPrimarySe {
  
  
  /**
   * The main function of memMarkPrimarySe class
   *
   * @param opt the MemOptType object
   * @param a the MemAlnRegType object
   * @param id the Long object
   */
  def memMarkPrimarySe(opt: MemOptType, a: Array[MemAlnRegType], id: Long) : Array[MemAlnRegType] = {
    var n: Int = 0
    if(a != null) n = a.length
    var i: Int = 0
    var j: Int = 0
    var tmp: Int = 0
    var k: Int = 0
    var z: Array[Int] = new Array[Int](n)
    var zIdx = 0
    //aVar, the returned value
    var aVar: Array[MemAlnRegType] = null
    if(n != 0) {
      i = 0
      while(i < n) {
        a(i).sub = 0
        a(i).secondary = -1
        a(i).hash = hash64((id + i.toLong))
        i += 1
      }
      //ks_introsort(mem_ars_hash, n, a)
      //#define alnreg_hlt(a, b) ((a).score > (b).score || ((a).score == (b).score && (a).hash < (b).hash))
      //aVar = a.sortWith( (x, y) => ((x.score > y.score) || ( x.score == y.score && (x.hash >>> 1) < (y.hash >>> 1) )  ) )
      aVar = a.sortBy(r => (- r.score, r.hash))
      tmp = opt.a + opt.b
      if((opt.oDel + opt.eDel) > tmp) {
        tmp = opt.oDel + opt.eDel
      }
      if((opt.oIns + opt.eIns) > tmp) {
        tmp = opt.oIns + opt.eIns
      }
      //kv_push()
      z(0) = 0
      zIdx += 1
      i = 1
      while(i < n) {
        var breakIdx: Int = zIdx
        var isBreak = false
        k = 0
       
        while(k < zIdx && !isBreak) {
          j = z(k)
          var bMax: Int = if(aVar(j).qBeg > aVar(i).qBeg) aVar(j).qBeg else aVar(i).qBeg
          var eMin: Int = if(aVar(j).qEnd < aVar(i).qEnd) aVar(j).qEnd else aVar(i).qEnd
          // have overlap
          if( eMin > bMax ) {
            var minL: Int = if ((aVar(i).qEnd - aVar(i).qBeg)<(aVar(j).qEnd - aVar(j).qBeg)) (aVar(i).qEnd - aVar(i).qBeg) else (aVar(j).qEnd - aVar(j).qBeg)
            //have significant overlap
            if((eMin - bMax)>= minL * opt.maskLevel) {
              if(aVar(j).sub == 0) {
                aVar(j).sub = aVar(i).score
              }
              if((aVar(j).score - aVar(i).score) <= tmp) aVar(j).subNum = aVar(j).subNum + 1
              breakIdx = k
              isBreak = true
            }
          }

          k += 1
        }

        if(breakIdx == zIdx) {
          z(zIdx) = i
          zIdx += 1
        }
        else {
          aVar(i).secondary = z(k)
        }

        i += 1
      }
    }
    aVar
  }

  def hash64( key: Long ) : Long = {
    var keyVar: Long = key
    keyVar += ~(keyVar << 32)
    keyVar ^= (keyVar >>> 22)
    keyVar += ~(keyVar << 13)
    keyVar ^= (keyVar >>> 8)
    keyVar += (keyVar << 3)
    keyVar ^= (keyVar >>> 15)
    keyVar += ~(keyVar <<27)
    keyVar ^= (keyVar >>> 31)
    keyVar
  }
}

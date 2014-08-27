package cs.ucla.edu.bwaspark.worker2

import scala.util.control.Breaks._
import scala.List
import scala.math.abs
import scala.collection.mutable.MutableList

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
  //??opt a list or a object??
  def memMarkPrimarySe(opt: MemOptType, a: MutableList[MemAlnRegType], id: Long) : MutableList[MemAlnRegType] = {
    val n: Int = a.length
    var i: Int = 0
    var j: Int = 0
    var tmp: Int = 0
    var k: Int = 0
    var z: MutableList[Int] = new MutableList
    //aVar, the returned value
    var aVar: MutableList[MemAlnRegType] = new MutableList
    if( n != 0) {
      for( i <- 0 until n ) {
	a(i).sub = 0
	a(i).secondary = -1
	a(i).hash = hash64((id + i.toLong))
	//println("i and hash value is "+ i + " "+ a(i).hash)
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
      z += 0
      for(i <- 1 until n) {
        var breakIdx: Int = z.size
	breakable {
	  for(k <- 0 until z.size) {
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
		  //println("#######should come here")
		}
		if((aVar(j).score - aVar(i).score) <= tmp) aVar(j).subNum = aVar(j).subNum + 1
		breakIdx = k
		break

	      }
	    }
	  }
	}
	if(breakIdx == z.size ) z += i
	else {
	  aVar(i).secondary = z(k)
	}
      }
    }
    aVar
  }

  private def hash64( key: Long ) : Long = {
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

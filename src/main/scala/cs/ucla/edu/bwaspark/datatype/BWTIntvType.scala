package cs.ucla.edu.bwaspark.datatype

class BWTIntvType(startPoint_l: Int, //uint32_t
                  endPoint_l: Int, //uint32_t
                  k_l: Long, //uint64_t
                  l_l: Long, //uint64_t
                  s_l: Long) { //uint64_t

  // endPoint - startPoint = length of the seed
  var startPoint = startPoint_l
  var endPoint = endPoint_l

  //a tuple (k, l, s) stands for a bi-interval, which is consistent with Heng Li's paper
  var k = k_l
  var l = l_l
  var s = s_l

  def print() {
    println ("start " + startPoint + ", end " + endPoint + ", (k, l, s) (" + k + ", " + l + ", " + s + ").")
  }

  def copy(intv: BWTIntvType) {
    startPoint = intv.startPoint
    endPoint = intv.endPoint
    k = intv.k
    l = intv.l
    s = intv.s
  }
}

package cs.ucla.edu.bwaspark.datatype

import scala.collection.mutable.MutableList

//Used for smemNext function
//bwt: bwt index and suffix array
//query: read
//start: the start point for forward and backward extension
//len: the length of the read
//matches: iteratively calling smemNext will accumulate this mutable list
//sub: temporary bi-interval array
//tmpVec0: temporary array 0 
//tmpVec1: temporary array 1 
class SMemItrType(bwt_c: BWTType,
                  query_c: Array[Byte], //uint8_t
                  start_i: Int,
                  len_i: Int,
                  matches_c: MutableList[BWTIntvType],
                  sub_c: MutableList[BWTIntvType],
                  tmpVec0_c: MutableList[BWTIntvType],
                  tmpVec1_c: MutableList[BWTIntvType]) {
  var bwt = bwt_c
  var query = query_c
  var start = start_i
  var len = len_i
  var matches = matches_c
  var sub = sub_c
  var tmpVec0 = tmpVec0_c
  var tmpVec1 = tmpVec1_c
}

package cs.ucla.edu.bwaspark.datatype

import scala.collection.mutable.MutableList

class CigarType {
  var cigarSegs: MutableList[CigarSegType] = new MutableList[CigarSegType]
  var cigarStr: String = new String
}


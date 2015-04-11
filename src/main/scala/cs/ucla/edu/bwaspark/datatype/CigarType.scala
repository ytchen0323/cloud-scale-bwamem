package cs.ucla.edu.bwaspark.datatype

import scala.collection.immutable.Vector

class CigarType {
  var cigarSegs: Vector[CigarSegType] = scala.collection.immutable.Vector.empty
  var cigarStr: String = new String

  /**
    *  Make a copy of the current object
    */
  def copy(): CigarType = {
    var cigar = new CigarType
    cigarSegs.foreach(seg => {
      cigar.cigarSegs = cigar.cigarSegs :+ seg.copy
    } )
    cigar.cigarStr = cigarStr
    cigar
  }
}


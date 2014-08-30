package cs.ucla.edu.bwaspark.datatype

class CigarSegType {
  var op: Byte = _           // The operation on this cigar segment
  var len: Int = _           // The length of this segment

  def copy(): CigarSegType = {
    var seg = new CigarSegType
    seg.op = op
    seg.len = len
    seg
  }
}


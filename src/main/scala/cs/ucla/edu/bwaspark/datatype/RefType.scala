package cs.ucla.edu.bwaspark.datatype

class RefType {
  var rBeg: Long = -1
  var rEnd: Long = -1
  var ref: Array[Byte] = _
  var len: Long = 0   // set to 0; This will influence the size of array allocation
}

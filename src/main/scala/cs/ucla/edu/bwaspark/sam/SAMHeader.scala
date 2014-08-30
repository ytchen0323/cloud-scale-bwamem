package cs.ucla.edu.bwaspark.sam

import cs.ucla.edu.bwaspark.datatype.BNTSeqType

object SAMHeader {
  var bwaReadGroupID = new String
  var readGroupLine = new String
  var packageVersion = "bwa-spark-0.2.0"
  var bwaPackageLine = new String 

  def bwaGenSAMHeader(bns: BNTSeqType): String = {
    def bwaPackageInfo() {
      bwaPackageLine = "@PG\tID:bwa\tPN:bwa\tVN:" + packageVersion + "\tCL:" + "bwa"
    }

    bwaPackageInfo
    var headerStr = new String
    var i = 0
    while(i < bns.n_seqs) {
      headerStr += "@SQ\tSN:" + bns.anns(i).name + "\tLN:" + bns.anns(i).len.toString  + '\n'
      i += 1
    }
    if(readGroupLine != "") headerStr = headerStr + readGroupLine + '\n'
    headerStr += bwaPackageLine + '\n'
    headerStr
  }

  def bwaSetReadGroup(str: String): Boolean = {
    val rgPattern = """@RG\s+ID:(\w+)""".r
    bwaReadGroupID = rgPattern findFirstIn str match {
      case Some (rgPattern(rgID)) => rgID
      case None => "Not matched"
    }

    if(bwaReadGroupID == "Not matched") false
    else {
      readGroupLine = str
      true
    }
  }
}


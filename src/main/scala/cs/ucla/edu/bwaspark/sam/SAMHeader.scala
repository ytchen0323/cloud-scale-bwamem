package cs.ucla.edu.bwaspark.sam

import cs.ucla.edu.bwaspark.datatype.BNTSeqType

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import scala.Serializable

class SAMHeader extends Serializable {
  var bwaReadGroupID = new String
  var readGroupLine = new String
  var packageVersion = new String
  var bwaPackageLine = new String 


  def bwaGenSAMHeader(bns: BNTSeqType, packageVerIn: String): String = {
    packageVersion = packageVerIn
    bwaPackageLine = "@PG\tID:bwa\tPN:bwa\tVN:" + packageVersion + "\tCL:" + "bwa"

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


  private def writeObject(out: ObjectOutputStream) {
    out.writeObject(bwaReadGroupID)
    out.writeObject(readGroupLine)
    out.writeObject(packageVersion)
    out.writeObject(bwaPackageLine)
  }

  private def readObject(in: ObjectInputStream) {
    bwaReadGroupID = in.readObject.asInstanceOf[String]
    readGroupLine = in.readObject.asInstanceOf[String]
    packageVersion = in.readObject.asInstanceOf[String]
    bwaPackageLine = in.readObject.asInstanceOf[String]
  }

  private def readObjectNoData() {

  }
    
}


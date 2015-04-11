package cs.ucla.edu.bwaspark.sam

import cs.ucla.edu.bwaspark.datatype.BNTSeqType

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.Date
import scala.Serializable

import htsjdk.samtools.{SAMFileHeader,SAMProgramRecord,SAMSequenceRecord,SAMSequenceDictionary,SAMReadGroupRecord}
import htsjdk.samtools.util.Iso8601Date

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


  def parseReadGroupString(str: String): SAMReadGroupRecord = {
    var id: String = null
    var cn: String = null
    var ds: String = null
    var dt: String = null
    var fo: String = null
    var ks: String = null
    var lb: String = null
    var pg: String = null
    var pi: String = null
    var pl: String = null
    var pu: String = null
    var sm: String = null

    val strArray = str.split('\t')
      
    for(strSeg <- strArray) {
      val op = strSeg.take(2)
      op match {
        case "ID" => id = strSeg.drop(3)
        case "CN" => cn = strSeg.drop(3) 
        case "DS" => ds = strSeg.drop(3) 
        case "DT" => dt = strSeg.drop(3)
        case "FO" => fo = strSeg.drop(3) 
        case "KS" => ks = strSeg.drop(3) 
        case "LB" => lb = strSeg.drop(3)
        //case "PG" => pg = strSeg.drop(3)   // Note: we omit PG field for now
        case "PI" => pi = strSeg.drop(3) 
        case "PL" => pl = strSeg.drop(3)
        case "PU" => pu = strSeg.drop(3) 
        case "SM" => sm = strSeg.drop(3) 
        case _ => None
      }
    }

    if(id != null) {
      val samReadGroup = new SAMReadGroupRecord(id)
      if(cn != null)
        samReadGroup.setSequencingCenter(cn)
      if(ds != null)
        samReadGroup.setDescription(ds)
      if(dt != null) 
        samReadGroup.setRunDate(new Iso8601Date(dt))
      if(fo != null)
        samReadGroup.setFlowOrder(fo)
      if(ks != null)
        samReadGroup.setKeySequence(ks)
      if(lb != null)
        samReadGroup.setLibrary(lb)
      if(pi != null)
        samReadGroup.setPredictedMedianInsertSize(pi.toInt)
      if(pl != null)
        samReadGroup.setPlatform(pl)
      if(pu != null)
        samReadGroup.setPlatformUnit(pu)
      if(sm != null)
        samReadGroup.setSample(sm)
    
      samReadGroup
    }
    else {
      println("[Error] Undefined group ID")
      exit(1)
    }
  }


  def bwaGenSAMHeader(bns: BNTSeqType, packageVerIn: String, readGroupString: String, samFileHeader: SAMFileHeader) {
    packageVersion = packageVerIn
    var samPG = new SAMProgramRecord("cs-bwamem")
    // NOTE setCommandLine() needs to be updated
    samPG.setProgramName("cs-bwamem")
    samPG.setProgramVersion(packageVersion)
    samPG.setCommandLine("cs-bwamem")
    samFileHeader.addProgramRecord(samPG)

    var samSeqDict = new SAMSequenceDictionary
    var i = 0
    while(i < bns.n_seqs) {
      samSeqDict.addSequence(new SAMSequenceRecord(bns.anns(i).name, bns.anns(i).len))
      i += 1
    }
    samFileHeader.setSequenceDictionary(samSeqDict)

    val samReadGroup = parseReadGroupString(readGroupString)
    samFileHeader.addReadGroup(samReadGroup)
  }


  def bwaSetReadGroup(str: String): Boolean = {
    val rgPattern = """@RG\s+ID:([\w_-]+)""".r
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


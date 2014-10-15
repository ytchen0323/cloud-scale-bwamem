package cs.ucla.edu.bwaspark.datatype

import java.io.FileInputStream
import java.nio.channels.FileChannel
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import scala.Serializable
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import cs.ucla.edu.bwaspark.datatype.BinaryFileReadUtil._

class BWTType extends Serializable {
  // Data Structure
  var primary: Long = _
  var L2: Array[Long] = new Array[Long](5)
  var seqLen: Long = _
  var bwtSize: Long = _
  var bwt: Array[Int] = _
  var cntTable: Array[Int] = new Array[Int](256)
  var saIntv: Int = _
  var numSa: Long = _
  var sa: Array[Long] = _

  /**
    *  Load the values of a BWTType object
    *  This function will load the bwt structure, the sa (suffix array) structure, and cnt table.
    *
    *  @param prefix the file path prefix, which will be used for reading <prefix>.bwt and <prefix>.sa
    */
  def load(prefix: String) {
    // Load the .bwt file
    BWTLoad(prefix + ".bwt")
    // Load the .sa file
    SALoad(prefix + ".sa")
    // Generate CNT table
    genCNTTable
  }

  /**
    *  Load the suffix array from a binary file
    *
    *  @param saFile the input file path
    */
  private def SALoad(saFile: String) {
    val conf = new Configuration
    val fs = FileSystem.get(conf)
    val path = new Path(saFile)
    if (fs.exists(path)) {
      val fc = fs.open(path)
      // Read primary in .sa file
      val primarySa = readLong(fc)
      assert(primarySa == primary, "SA-BWT inconsistency: primary is not the same.")

      // Skipped 4 * sizeof(Long)
      for(i <- 0 to 3)
        readLong(fc)

      // Read saIntv member variable in BWTType
      saIntv = readLong(fc).asInstanceOf[Int]  // Long -> Int (done in the original c code)

      // Read seqLen in .sa file
      val seqLenSa = readLong(fc)
      assert(seqLenSa == seqLen, "SA-BWT inconsistency: seq_len is not the same.")

      numSa = (seqLen + saIntv) / saIntv;
      sa = readLongArray(fc, numSa.asInstanceOf[Int], 1)   // numSa: Long -> Int
      sa(0) = -1    

      fc.close
    }
    else {
      val fc = new FileInputStream(saFile).getChannel
      // Read primary in .sa file
      val primarySa = readLong(fc)
      assert(primarySa == primary, "SA-BWT inconsistency: primary is not the same.")

      // Skipped 4 * sizeof(Long)
      for(i <- 0 to 3)
        readLong(fc)

      // Read saIntv member variable in BWTType
      saIntv = readLong(fc).asInstanceOf[Int]  // Long -> Int (done in the original c code)

      // Read seqLen in .sa file
      val seqLenSa = readLong(fc)
      assert(seqLenSa == seqLen, "SA-BWT inconsistency: seq_len is not the same.")

      numSa = (seqLen + saIntv) / saIntv;
      sa = readLongArray(fc, numSa.asInstanceOf[Int], 1)   // numSa: Long -> Int
      sa(0) = -1    

      fc.close
    }

    // Debugging message
    //println("saIntv: " + saIntv)
    //println("numSa: " + numSa)
    //for(i <- 0 to 19)
      //println("sa(" + i + "): " + sa(i))
  }

  /**
    *  Load the bwt (Burrows Wheeler Transform) structure from a binary file
    *
    *  @param bwtFile the input file path
    */
  private def BWTLoad(bwtFile: String) {
    val conf = new Configuration
    val fs = FileSystem.get(conf)
    val path = new Path(bwtFile)
    if (fs.exists(path)) {
      var fc = fs.open(path)
      var fileSize = fs.getFileStatus(path).getLen
      // Read primary member variable in BWTType
      primary = readLong(fc)

      // Read L[1] - L[4] member variables in BWTType
      L2(0) = 0
      for(i <- 1 to 4) 
        L2(i) = readLong(fc)
      seqLen = L2(4)

      // Read the bwt array in BWTType
      bwtSize = (fileSize - 8 * 5) >> 2
      bwt = readIntArray(fc, bwtSize.asInstanceOf[Int], 0)

      fc.close
    }
    else {
      var fc = new FileInputStream(bwtFile).getChannel
      var fileSize = fc.size
      // Read primary member variable in BWTType
      primary = readLong(fc)

      // Read L[1] - L[4] member variables in BWTType
      L2(0) = 0
      for(i <- 1 to 4) 
        L2(i) = readLong(fc)
      seqLen = L2(4)

      // Read the bwt array in BWTType
      bwtSize = (fileSize - 8 * 5) >> 2
      bwt = readIntArray(fc, bwtSize.asInstanceOf[Int], 0)

      fc.close
    }

    // Debugging messages
    //println("File size: " + fileSize)
    //println("primary: " + primary)
    //println("bwtSize: " + bwtSize)
    //println("seqLen: " + seqLen)
    //for(i <- 0 to 4) 
      //println("L2(" + i + "): " + L2(i))
    //for(i <- 0 to 19)
      //println("bwt(" + i + "): " + bwt(i))
  }

  /**
    *  Utility function: Boolean -> Int
    *
    *  @param b the input boolean variable
    */
  implicit def bool2int(b:Boolean) = if (b) 1 else 0


  /**
    *  Generate teh CNT table for a BWTType object
    *
    */
  private def genCNTTable() {
    for(i <- 0 to 255) {
      var x = 0
      for(j <- 0 to 3) {
        x = x | ((((i&3) == j) + ((i>>>2&3) == j) + ((i>>>4&3) == j) + (i>>>6 == j)) << (j<<3))
      }   
      cntTable(i) = x
    }

    // Debugging messages
    //for(i <- 0 to 255) 
      //println("cntTable(" + i + "): " + cntTable(i))
  }

  private def writeObject(out: ObjectOutputStream) {
    out.writeLong(primary)
    out.writeObject(L2)
    out.writeLong(seqLen)
    out.writeLong(bwtSize)
    out.writeObject(bwt)
    out.writeObject(cntTable)
    out.writeInt(saIntv)
    out.writeLong(numSa)
    out.writeObject(sa)
  }
  
  private def readObject(in: ObjectInputStream) {
    primary = in.readLong
    L2 = in.readObject.asInstanceOf[Array[Long]]
    seqLen = in.readLong
    bwtSize = in.readLong
    bwt = in.readObject.asInstanceOf[Array[Int]]
    cntTable = in.readObject.asInstanceOf[Array[Int]]
    saIntv = in.readInt
    numSa = in.readLong
    sa = in.readObject.asInstanceOf[Array[Long]]
  }

  private def readObjectNoData() {

  }
}


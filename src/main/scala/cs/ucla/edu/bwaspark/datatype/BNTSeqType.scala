package cs.ucla.edu.bwaspark.datatype

import java.io._
import scala.Serializable
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

class BNTSeqType extends Serializable {
  //length of contents in .pac file
  var l_pac: Long = _

  //length of contents in .ann file
  var n_seqs: Int = _

  //!!!to add!!!
  var seed: Int = _

  //maintaining contents in .ann file
  var anns: Array[BNTAnnType] = _

  //length of contents in .amb file
  var n_holes: Int = _

  //maintaining contents in .amb file
  var ambs: Array[BNTAmbType] = _

  //There is a file pointer to .pac file in original BWA,
  //but it seems to be useless

  //loading .ann .amb files
  def load(prefix: String) {

    //define a loader for .ann file
    def annLoader(filename: String): (Array[BNTAnnType], Long, Int, Int) = {
      val conf = new Configuration
      val fs = FileSystem.get(conf)
      val path = new Path(filename)
      var annBufferedReader: BufferedReader = null
      if (fs.exists(path)) {
        annBufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)))
      }
      else {
        annBufferedReader = new BufferedReader(new FileReader(filename)) //file reader
      }
      val headLine = annBufferedReader.readLine.split(" ") //the first line of the file, specify three variables: l_pac, n_seqs, and seed
      assert(headLine.length == 3)
      val l_pac = headLine(0).toLong
      val n_seqs = headLine(1).toInt
      val seed = headLine(2).toInt
      val anns = new Array[BNTAnnType](n_seqs) //create an array for the contents of .ann file
      for (i <- 0 until n_seqs) { //fill in each element
        val firstLine = annBufferedReader.readLine.split(" ")
        val gi = firstLine(0).toInt
        val name = firstLine(1)
        val anno = "" // fix me! No annotation at all!
        val secondLine = annBufferedReader.readLine.split(" ")
        assert(secondLine.length == 3) 
        val offset = secondLine(0).toLong
        val len = secondLine(1).toInt
        val n_ambs = secondLine(2).toInt
        anns(i) = new BNTAnnType(offset, len, n_ambs, gi, name, anno)
      }
      annBufferedReader.close() //close file and return
      (anns, l_pac, n_seqs, seed)
    }
    //call annLoader to assign variables: anns, l_pac, n_seqs, and seed
    val annResult = annLoader(prefix + ".ann")
    anns = annResult._1
    l_pac = annResult._2
    n_seqs = annResult._3
    seed = annResult._4

    //define a loader for .amb file
    def ambLoader(filename: String): (Array[BNTAmbType], Int) = {
      val conf = new Configuration
      val fs = FileSystem.get(conf)
      val path = new Path(filename)
      var ambBufferedReader: BufferedReader = null
      if (fs.exists(path)) {
        ambBufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)))
      }
      else {
        ambBufferedReader = new BufferedReader(new FileReader(filename)) //file reader
      }
      val headLine = ambBufferedReader.readLine.split(" ").map(str => str.toLong) //the first line of the file, specify three variables: l_pac, n_seqs, and n_holes; l_pac and n_seqs are the same as those in .ann file
      assert(headLine.length == 3)
      val n_holes = headLine(2).toInt
      var ambs = new Array[BNTAmbType](n_holes)
      for (i <- 0 until n_holes) {
        val currLine = ambBufferedReader.readLine.split(" ")
        assert(currLine.length == 3) 
        val offset = currLine(0).toLong
        val len = currLine(1).toInt
        val amb = currLine(2)(0)
        ambs(i) = new BNTAmbType(offset, len, amb)
      }
      ambBufferedReader.close() //close file and return
      (ambs, n_holes)
    }
    //call ambLoader to assign variables: ambs and n_holes
    val ambResult = ambLoader(prefix + ".amb") 
    ambs = ambResult._1
    n_holes = ambResult._2
  }

  private def writeObject(out: ObjectOutputStream) {
    out.writeLong(l_pac)
    out.writeInt(n_seqs)
    out.writeInt(seed)
    out.writeObject(anns)
    out.writeInt(n_holes)
    out.writeObject(ambs)
  }

  private def readObject(in: ObjectInputStream) {
    l_pac = in.readLong
    n_seqs = in.readInt
    seed = in.readInt
    anns = in.readObject.asInstanceOf[Array[BNTAnnType]]
    n_holes = in.readInt
    ambs = in.readObject.asInstanceOf[Array[BNTAmbType]]
  }

  private def readObjectNoData() {

  }

}             

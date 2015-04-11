package cs.ucla.edu.bwaspark.datatype

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import scala.Serializable

class BNTAmbType (offset_l: Long,
                 len_i: Int,
                 amb_c: Char) extends Serializable {
  var offset = offset_l
  var len = len_i
  var amb = amb_c

  private def writeObject(out: ObjectOutputStream) {
    out.writeLong(offset)
    out.writeInt(len)
    out.writeInt(amb)
  }

  private def readObject(in: ObjectInputStream) {
    offset = in.readLong
    len = in.readInt
    amb = in.readChar
  }

  private def readObjectNoData() {

  }

}

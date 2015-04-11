package cs.ucla.edu.bwaspark.datatype

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import scala.Serializable

class BNTAnnType (offset_l: Long,
                 len_i: Int,
                 n_ambs_i: Int,
                 gi_i: Int,  //uint32_t
                 name_s: String,
                 anno_s: String) extends Serializable {
  var offset = offset_l
  var len = len_i
  var n_ambs = n_ambs_i
  var gi = gi_i
  var name = name_s
  var anno = anno_s

  private def writeObject(out: ObjectOutputStream) {
    out.writeLong(offset)
    out.writeInt(len)
    out.writeInt(n_ambs)
    out.writeInt(gi)
    out.writeObject(name)
    out.writeObject(anno)
  }

  private def readObject(in: ObjectInputStream) {
    offset = in.readLong
    len = in.readInt
    n_ambs = in.readInt
    gi = in.readInt
    name = in.readObject.asInstanceOf[String]
    anno = in.readObject.asInstanceOf[String]
  }

  private def readObjectNoData() {

  }

}

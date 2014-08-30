package cs.ucla.edu.bwaspark.datatype

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import scala.Serializable

class PeStatPrepType extends Serializable {
  var dir: Int = -1        // direction
  var dist: Int = -1       // distance

  private def writeObject(out: ObjectOutputStream) {
    out.writeInt(dir)
    out.writeInt(dist)
  }

  private def readObject(in: ObjectInputStream) {
    dir = in.readInt
    dist = in.readInt
  }

  private def readObjectNoData() {

  }

}


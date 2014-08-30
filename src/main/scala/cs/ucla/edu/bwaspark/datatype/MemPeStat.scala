package cs.ucla.edu.bwaspark.datatype

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import scala.Serializable

//Original data structure: mem_pestat_t in bwamem.h
class MemPeStat extends Serializable {
  var low: Int = 0
  var high: Int = 0
  var failed: Int = 0
  var avg: Double = 0
  var std: Double = 0

  private def writeObject(out: ObjectOutputStream) {
    out.writeInt(low)
    out.writeInt(high)
    out.writeInt(failed)
    out.writeDouble(avg)
    out.writeDouble(std)
  }

  private def readObject(in: ObjectInputStream) {
    low = in.readInt
    high = in.readInt
    failed = in.readInt
    avg = in.readDouble
    std = in.readDouble
  }

  private def readObjectNoData() {

  }

}

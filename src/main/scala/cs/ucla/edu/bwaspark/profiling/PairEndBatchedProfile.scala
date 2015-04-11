package cs.ucla.edu.bwaspark.profiling

import scala.Serializable

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.ObjectStreamException

import cs.ucla.edu.bwaspark.datatype.PairEndReadType

class PairEndBatchedProfile extends Serializable {
  var pairEndReadArray: Array[PairEndReadType] = _
  var swBatchTimeBreakdown: SWBatchTimeBreakdown = _

  private def writeObject(out: ObjectOutputStream) {
    out.writeObject(pairEndReadArray)
    out.writeObject(swBatchTimeBreakdown)
  }

  private def readObject(in: ObjectInputStream) {
    pairEndReadArray = in.readObject.asInstanceOf[Array[PairEndReadType]]
    swBatchTimeBreakdown = in.readObject.asInstanceOf[SWBatchTimeBreakdown]
  }

  private def readObjectNoData() {

  }
}

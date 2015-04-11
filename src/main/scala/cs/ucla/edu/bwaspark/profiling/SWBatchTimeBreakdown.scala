package cs.ucla.edu.bwaspark.profiling

import scala.Serializable

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.ObjectStreamException

class SWBatchTimeBreakdown extends Serializable {
  var isFPGA: Boolean = true
  var initSWBatchTime: Long  = 0
  var SWBatchRuntime: Long  = 0
  var SWBatchOnFPGA: Long  = 0
  var postProcessSWBatchTime: Long  = 0
  var FPGADataPreProcTime: Long = 0
  var FPGARoutineRuntime: Long = 0
  var FPGADataPostProcTime: Long = 0
  var FPGATaskNum: Long = 0
  var CPUTaskNum: Long = 0
  var generatedChainTime: Long = 0
  var filterChainTime: Long = 0
  var chainToAlnTime: Long = 0
  var sortAndDedupTime: Long = 0

  private def writeObject(out: ObjectOutputStream) {
    out.writeBoolean(isFPGA)
    out.writeLong(initSWBatchTime)
    out.writeLong(SWBatchRuntime)
    out.writeLong(SWBatchOnFPGA)
    out.writeLong(postProcessSWBatchTime)
    out.writeLong(FPGADataPreProcTime)
    out.writeLong(FPGARoutineRuntime)
    out.writeLong(FPGADataPostProcTime)
    out.writeLong(FPGATaskNum)
    out.writeLong(CPUTaskNum)
    out.writeLong(generatedChainTime)
    out.writeLong(filterChainTime)
    out.writeLong(chainToAlnTime)
    out.writeLong(sortAndDedupTime)
  }

  private def readObject(in: ObjectInputStream) {
    isFPGA = in.readBoolean
    initSWBatchTime = in.readLong
    SWBatchRuntime = in.readLong
    SWBatchOnFPGA = in.readLong
    postProcessSWBatchTime = in.readLong
    FPGADataPreProcTime = in.readLong
    FPGARoutineRuntime = in.readLong
    FPGADataPreProcTime = in.readLong
    FPGATaskNum = in.readLong
    CPUTaskNum = in.readLong
    generatedChainTime = in.readLong
    filterChainTime = in.readLong
    chainToAlnTime = in.readLong
    sortAndDedupTime = in.readLong
  }

  private def readObjectNoData() {

  }
}

package cs.ucla.edu.bwaspark.jni

import cs.ucla.edu.bwaspark.datatype.{MemOptType, MemPeStat, FASTQSingleNode}

class MateSWJNI {
  @native def mateSWJNI(opt: MemOptType, pacLen: Long, pes: Array[MemPeStat], groupSize: Int, seqsPairs: Array[SeqSWType], 
                        mateSWArray: Array[MateSWType], refSWArray: Array[RefSWType], refSWArraySize: Array[Int]): Array[MateSWType]
}


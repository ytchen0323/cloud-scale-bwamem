package cs.ucla.edu.bwaspark.jni

class SWExtendFPGAJNI {
  @native def swExtendFPGAJNI(taskNum: Int, SWArray: Array[Byte]): Array[Short]
}

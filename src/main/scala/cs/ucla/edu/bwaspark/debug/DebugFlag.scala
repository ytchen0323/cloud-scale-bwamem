package cs.ucla.edu.bwaspark.debug

//this standalone object is for debug handling
//which has only one variable for marking debug level
//different debug level will generate different printing information

object DebugFlag {

  //0: no debug output at all
  //1: all debug information

  var debugLevel: Int = _
  var debugBWTSMem : Boolean = false


}

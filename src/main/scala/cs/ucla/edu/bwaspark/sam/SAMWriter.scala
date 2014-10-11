package cs.ucla.edu.bwaspark.sam

import java.io.BufferedWriter
import java.nio.charset.Charset
import java.nio.file.{Files,Path,Paths}


class SAMWriter(outFile: String) {
  var writer: BufferedWriter = _

  def init() {
    writer = Files.newBufferedWriter(Paths.get(outFile), Charset.forName("utf-8"))
  }

  def writeString(str: String) {
    writer.write(str, 0, str.length)
  }

  def writeStringArray(strArray: Array[String]) {
    var i = 0
    while(i < strArray.length) {
      writer.write(strArray(i), 0, strArray(i).length)
      i += 1
    }
    writer.flush
  }

  def close() {
    writer.close
  }
}


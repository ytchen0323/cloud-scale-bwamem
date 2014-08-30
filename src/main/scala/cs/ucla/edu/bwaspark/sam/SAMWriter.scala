package cs.ucla.edu.bwaspark.sam

import java.io.{PrintWriter, File}

class SAMWriter(outFile: String) {
  var writer: PrintWriter = _

  def init() {
    writer = new PrintWriter(new File(outFile))
  }

  def writeString(str: String) {
    writer.write(str)
  }

  def close() {
    writer.close
  }
}


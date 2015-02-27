package cs.ucla.edu.bwaspark.sam

import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.nio.charset.Charset

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;


class SAMHDFSWriter(outFile: String) {
  var writer: BufferedWriter = _

  def init() {
    val fs = FileSystem.get(new Configuration)
    writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outFile + "/header"))));
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



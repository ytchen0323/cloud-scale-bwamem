/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


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



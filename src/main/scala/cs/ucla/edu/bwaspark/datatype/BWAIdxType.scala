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


package cs.ucla.edu.bwaspark.datatype

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.{FileInputStream, IOException}
import java.nio.channels.FileChannel
import cs.ucla.edu.bwaspark.datatype.BinaryFileReadUtil._
import scala.Serializable
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

//BWAIdxType: maintaining all the information of BWA Index generated from FastA Reference
class BWAIdxType extends Serializable {  

  //1st: BWTType(".bwt", ".sa" files)
  var bwt: BWTType = _

  //2nd: BNTSeqType(".ann", ".amb" files)
  var bns: BNTSeqType = _

  //3rd: PACType(".pac" file)
  var pac: Array[Byte] = _  //uint8_t[]

  //loading files into fields
  //prefix: prefix of filenames
  //which: !!!to add!!!
  def load(prefix: String) { load(prefix, 0) }
  def load(prefix: String, which: Int) {
    //There is a function called "bwa_idx_infer_prefix" in original BWA,
    //but it seems to be useless

    //loading bwt
    //!!!In the beginning, set all as true
    //if (which & BWA_IDX_BWT) {
    if (true) {
      bwt = new BWTType
      bwt.load(prefix)
    }

    //loading bns
    //!!!In the beginning, set all as true
    //if (which & BWA_IDX_BNS) {
    if (true) {
      bns = new BNTSeqType
      bns.load(prefix)
      
      //loading pac
      //!!!In the beginning, set all as true
      //if (which & BWA_IDX_PAC) {
      if (true) {
        def pacLoader(filename: String, length: Long): Array[Byte] = {
          //to add: reading binary file
          val conf = new Configuration
          val fs = FileSystem.get(conf)
          val path = new Path(filename)
          //var reader: AnyRef = null
          if (fs.exists(path)) {
            val reader = fs.open(path)
            var pac = readByteArray(reader, (length/4+1).toInt, 0)          
            pac
          }
          else {
            val reader = new FileInputStream(filename).getChannel
            var pac = readByteArray(reader, (length/4+1).toInt, 0)          
            pac
          }
        }
        pac = pacLoader(prefix+".pac", bns.l_pac)
      }
    }
  }

  private def writeObject(out: ObjectOutputStream) {
    out.writeObject(bwt)
    out.writeObject(bns)
    out.writeObject(pac) 
  }

  private def readObject(in: ObjectInputStream) {
    bwt = in.readObject.asInstanceOf[BWTType]
    bns = in.readObject.asInstanceOf[BNTSeqType]
    pac = in.readObject.asInstanceOf[Array[Byte]]
  }

  private def readObjectNoData() {

  }

}

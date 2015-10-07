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

import scala.Serializable

import org.apache.avro.io._
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter

import cs.ucla.edu.avro.fastq._

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.ObjectStreamException

class ReadType extends Serializable {
  var seq: FASTQRecord = _
  var regs: Array[MemAlnRegType] = _

  private def writeObject(out: ObjectOutputStream) {
    out.writeObject(regs)
    val writer = new SpecificDatumWriter[FASTQRecord](classOf[FASTQRecord])
    val encoder = EncoderFactory.get.binaryEncoder(out, null)
    writer.write(seq, encoder)
    encoder.flush()
  }

  private def readObject(in: ObjectInputStream) {
    regs = in.readObject.asInstanceOf[Array[MemAlnRegType]]
    val reader = new SpecificDatumReader[FASTQRecord](classOf[FASTQRecord]);
    val decoder = DecoderFactory.get.binaryDecoder(in, null);
    seq = reader.read(null, decoder).asInstanceOf[FASTQRecord]
  }

  private def readObjectNoData() {

  }
}


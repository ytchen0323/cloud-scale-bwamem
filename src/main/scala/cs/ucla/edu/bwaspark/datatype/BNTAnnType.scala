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
import scala.Serializable

class BNTAnnType (offset_l: Long,
                 len_i: Int,
                 n_ambs_i: Int,
                 gi_i: Int,  //uint32_t
                 name_s: String,
                 anno_s: String) extends Serializable {
  var offset = offset_l
  var len = len_i
  var n_ambs = n_ambs_i
  var gi = gi_i
  var name = name_s
  var anno = anno_s

  private def writeObject(out: ObjectOutputStream) {
    out.writeLong(offset)
    out.writeInt(len)
    out.writeInt(n_ambs)
    out.writeInt(gi)
    out.writeObject(name)
    out.writeObject(anno)
  }

  private def readObject(in: ObjectInputStream) {
    offset = in.readLong
    len = in.readInt
    n_ambs = in.readInt
    gi = in.readInt
    name = in.readObject.asInstanceOf[String]
    anno = in.readObject.asInstanceOf[String]
  }

  private def readObjectNoData() {

  }

}

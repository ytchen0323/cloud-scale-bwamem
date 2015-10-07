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

//Original data structure: mem_pestat_t in bwamem.h
class MemPeStat extends Serializable {
  var low: Int = 0
  var high: Int = 0
  var failed: Int = 0
  var avg: Double = 0
  var std: Double = 0

  private def writeObject(out: ObjectOutputStream) {
    out.writeInt(low)
    out.writeInt(high)
    out.writeInt(failed)
    out.writeDouble(avg)
    out.writeDouble(std)
  }

  private def readObject(in: ObjectInputStream) {
    low = in.readInt
    high = in.readInt
    failed = in.readInt
    avg = in.readDouble
    std = in.readDouble
  }

  private def readObjectNoData() {

  }

}

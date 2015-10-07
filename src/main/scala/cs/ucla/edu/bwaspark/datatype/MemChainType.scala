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

import scala.collection.mutable.MutableList

class MemSeedType(rbeg_i: Long, qbeg_i: Int, len_i: Int) {
  var rBeg: Long = rbeg_i
  var qBeg: Int = qbeg_i
  var len: Int = len_i
}

class MemChainType(pos_i: Long, seeds_i: MutableList[MemSeedType]) {
  var pos: Long = pos_i
  var seeds: MutableList[MemSeedType] = seeds_i
  var seedsRefArray: Array[MemSeedType] = _

  def print() {
    println("The reference position of the chain: " + pos)
    seeds.map (ele => println("Ref Begin: " + ele.rBeg + ", Query Begin: " + ele.qBeg + ", Length: " + ele.len))
  }

}

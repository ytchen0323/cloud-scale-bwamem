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

class BWTIntvType(startPoint_l: Int, //uint32_t
                  endPoint_l: Int, //uint32_t
                  k_l: Long, //uint64_t
                  l_l: Long, //uint64_t
                  s_l: Long) { //uint64_t

  // endPoint - startPoint = length of the seed
  var startPoint = startPoint_l
  var endPoint = endPoint_l

  //a tuple (k, l, s) stands for a bi-interval, which is consistent with Heng Li's paper
  var k = k_l
  var l = l_l
  var s = s_l

  def print() {
    println ("start " + startPoint + ", end " + endPoint + ", (k, l, s) (" + k + ", " + l + ", " + s + ").")
  }

  def copy(intv: BWTIntvType) {
    startPoint = intv.startPoint
    endPoint = intv.endPoint
    k = intv.k
    l = intv.l
    s = intv.s
  }
}

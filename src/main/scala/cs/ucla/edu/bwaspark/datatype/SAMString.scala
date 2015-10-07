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

class SAMString {
  var str: Array[Char] = new Array[Char](8192)
  var idx: Int = 0
  var size: Int = 8192

  def addCharArray(in: Array[Char]) {
    if((idx + in.size + 1) >= size) {
      size = size << 2
      val old = str
      str = new Array[Char](size)
      old.copyToArray(str, 0, idx + 1)
    }
 
    var i = 0
    while(i < in.size) {
      str(idx) = in(i)
      i += 1
      idx += 1
    }
  }
 
  def addChar(c: Char) {
    if((idx + 1) >= size) {
      size = size << 2
      val old = str
      str = new Array[Char](size)
      old.copyToArray(str, 0, idx + 1)
    }

    str(idx) = c
    idx += 1
  }

  override def toString = new String(str, 0, idx)

}


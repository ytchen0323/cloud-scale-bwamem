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


package cs.ucla.edu.bwaspark.jni

class RefSWType {
  var readIdx: Int = -1
  var pairIdx: Int = -1
  var regIdx: Int = -1
  var rBegArray: Array[Long] = _
  var rEndArray: Array[Long] = _
  var lenArray: Array[Long] = _
  var ref0: Array[Byte] = _
  var ref1: Array[Byte] = _
  var ref2: Array[Byte] = _
  var ref3: Array[Byte] = _
}

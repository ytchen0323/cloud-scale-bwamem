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

import cs.ucla.edu.bwaspark.datatype.{MemOptType, MemPeStat, FASTQSingleNode}

class MateSWJNI {
  @native def mateSWJNI(opt: MemOptType, pacLen: Long, pes: Array[MemPeStat], groupSize: Int, seqsPairs: Array[SeqSWType], 
                        mateSWArray: Array[MateSWType], refSWArray: Array[RefSWType], refSWArraySize: Array[Int]): Array[MateSWType]
}


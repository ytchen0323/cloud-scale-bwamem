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


package cs.ucla.edu.bwaspark.broadcast

import java.io._                                                                                  
                                                                                                  
import org.apache.spark.broadcast.Broadcast                                                       
import org.apache.spark.SparkException                                                            
import org.apache.spark.Logging                                                                   
import org.apache.spark.util.Utils                                                                
                                                                                                  
import scala.reflect.ClassTag

import cs.ucla.edu.bwaspark.datatype.BWAIdxType

class ReferenceBroadcast(bd: Broadcast[BWAIdxType], isFromLocal: Boolean, path: String) extends Serializable {
  lazy val reference: BWAIdxType = init()

  private def init(): BWAIdxType = {
    this.synchronized {
      if(isFromLocal) {
        val ref = new BWAIdxType
        println("Load reference genome")
        ref.load(path, 0)
        ref
      }
      else 
        bd.value
    }
  }

  def value() = {
    reference
  }
}


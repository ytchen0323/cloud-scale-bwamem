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


package cs.ucla.edu.bwaspark.dnaseq

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.net.URI

object Sort extends Serializable {
  def apply(sc: SparkContext, hdfsAddress: String, alignmentsRootPath: String, coalesceFactor: Int) = {
    val conf = new Configuration
    val fs = FileSystem.get(new URI(hdfsAddress), conf)
    val paths = fs.listStatus(new Path(alignmentsRootPath)).map(ele => ele.getPath)
    val totalFilePartitions = paths.flatMap(p => fs.listStatus(p)).map(ele => ele.getPath).size
    println("Total number of new file partitions" + (totalFilePartitions/coalesceFactor))
    var adamRecords: RDD[AlignmentRecord] = new ADAMContext(sc).loadAlignmentsFromPaths(paths)
    adamRecords.coalesce(totalFilePartitions/coalesceFactor).adamSortReadsByReferencePosition()
  }
}

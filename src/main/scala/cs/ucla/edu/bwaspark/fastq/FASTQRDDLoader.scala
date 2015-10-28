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


package cs.ucla.edu.bwaspark.fastq

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.List

import cs.ucla.edu.avro.fastq._

import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.avro.{AvroParquetInputFormat, AvroReadSupport}
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.parquet.filter.UnboundRecordFilter

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.conf.Configuration

import java.util.logging.{Level, Logger}

class FASTQRDDLoader(sc: SparkContext, rootFilePath: String, numDir: Int) {
   // Only prints non-null amino acids
   def FASTQPrinter(rec: FASTQRecord) = {
      println(rec.getSeqLength())
   }

   // Not used at the current stage
   // Cannot find a way to access HDFS directly from Spark...
   def findFiles(fs: FileSystem, path: Path): Seq[Path] = {
      val statuses = fs.listStatus(path)
      val dirs = statuses.filter(s => s.isDirectory).map(s => s.getPath)
      dirs.toSeq ++ dirs.flatMap(p => findFiles(fs, p))
   }

   /**
     *  Load the FASTQ from HDFS into RDD
     *
     *  @param path the input HDFS path
     */
   def RDDLoad(path: String): RDD[FASTQRecord] = {
      val job = new Job(sc.hadoopConfiguration)
      ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[FASTQRecord]])
      val records = sc.newAPIHadoopFile(path, classOf[ParquetInputFormat[FASTQRecord]], classOf[Void], classOf[FASTQRecord], ContextUtil.getConfiguration(job)).map(p => p._2)
      records
   }

   /**
     *  Load all directories from HDFS of the given FASTQ file
     *  NOTE: Currently we cannot access the HDFS directory tree structure
     *  We ask users to input the number of subdirectories manually...
     *  This should be changed later.  
     */
   def RDDLoadAll(): RDD[FASTQRecord] = {
      //val fs = FileSystem.get(sc.hadoopConfiguration)
      //val paths = findFiles(fs, new Path(rootFilePath))

      var i = 0
      var paths:List[String] = List()
      
      // numDir: the number of sub-directories in HDFS (given from user)
      // The reason is that currently we cannot directly fetch the directory information from HDFS
      while(i < numDir) {
         val path = rootFilePath + "/" + i.toString
         paths = path :: paths
         i += 1
      }

      val records = sc.union(paths.map(p => RDDLoad(p)))
      records
   } 

   /**
     *  Load the Pair-End FASTQ from HDFS into RDD
     *
     *  @param path the input HDFS path
     */
   def PairEndRDDLoad(path: String): RDD[PairEndFASTQRecord] = {
      val job = new Job(sc.hadoopConfiguration)
      ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[PairEndFASTQRecord]])
      val records = sc.newAPIHadoopFile(path, classOf[ParquetInputFormat[PairEndFASTQRecord]], classOf[Void], classOf[PairEndFASTQRecord], ContextUtil.getConfiguration(job)).map(p => p._2)
      records
   }

   /**
     *  Load all directories from HDFS of the given Pair-End FASTQ file
     *  NOTE: Currently we cannot access the HDFS directory tree structure
     *  We ask users to input the number of subdirectories manually...
     *  This should be changed later.  
     */
   def PairEndRDDLoadAll(): RDD[PairEndFASTQRecord] = {
      //val fs = FileSystem.get(sc.hadoopConfiguration)
      //val paths = findFiles(fs, new Path(rootFilePath))

      var i = 0
      var paths:List[String] = List()
      
      // numDir: the number of sub-directories in HDFS (given from user)
      // The reason is that currently we cannot directly fetch the directory information from HDFS
      while(i < numDir) {
         val path = rootFilePath + "/" + i.toString
         paths = path :: paths
         i += 1
      }

      val records = sc.union(paths.map(p => PairEndRDDLoad(p)))
      records
   } 

   /**
     *  Load the Pair-End FASTQ from HDFS into RDD in a batched fashion
     *
     *  @param nextFolderIdx the index of the next folder to be read
     *  @param batchFolderNum the number of folders read in this batch
     */
   def PairEndRDDLoadOneBatch(nextFolderIdx: Int, batchFolderNum: Int): RDD[PairEndFASTQRecord] = {
      var i = nextFolderIdx
      var endFolderIdx = nextFolderIdx + batchFolderNum
      var paths:List[String] = List()
      
      // numDir: the number of sub-directories in HDFS (given from user)
      // The reason is that currently we cannot directly fetch the directory information from HDFS
      while(i < endFolderIdx) {
         val path = rootFilePath + "/" + i.toString
         paths = path :: paths
         i += 1
      }

      val records = sc.union(paths.map(p => PairEndRDDLoad(p)))
      records
   } 

   /**
     *  Load the Single-End FASTQ from HDFS into RDD in a batched fashion
     *
     *  @param nextFolderIdx the index of the next folder to be read
     *  @param batchFolderNum the number of folders read in this batch
     */
   def SingleEndRDDLoadOneBatch(nextFolderIdx: Int, batchFolderNum: Int): RDD[FASTQRecord] = {
      var i = nextFolderIdx
      var endFolderIdx = nextFolderIdx + batchFolderNum
      var paths:List[String] = List()
      
      // numDir: the number of sub-directories in HDFS (given from user)
      // The reason is that currently we cannot directly fetch the directory information from HDFS
      while(i < endFolderIdx) {
         val path = rootFilePath + "/" + i.toString
         paths = path :: paths
         i += 1
      }

      val records = sc.union(paths.map(p => RDDLoad(p)))
      records
   } 
}

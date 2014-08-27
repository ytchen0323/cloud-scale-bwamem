package cs.ucla.edu.bwaspark.fastq

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.List

import cs.ucla.edu.avro.fastq._

import parquet.hadoop.ParquetInputFormat
import parquet.avro.{AvroParquetInputFormat, AvroReadSupport}
import org.apache.hadoop.mapreduce.Job
import parquet.hadoop.util.ContextUtil
import parquet.filter.UnboundRecordFilter

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
}

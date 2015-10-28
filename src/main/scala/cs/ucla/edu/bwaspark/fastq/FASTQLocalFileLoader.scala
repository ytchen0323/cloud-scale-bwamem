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

import java.io.{File, FileReader, InputStreamReader, BufferedReader, IOException, FileNotFoundException}
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{Charset, CharsetEncoder, CharacterCodingException}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import scala.util.control.Breaks._
import scala.List
import scala.collection.parallel.mutable.ParArray
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{Success, Failure}
import scala.concurrent.duration._

import cs.ucla.edu.avro.fastq._

import org.apache.hadoop.mapreduce.Job
//import org.apache.parquet.hadoop.ParquetOutputFormat
//import org.apache.parquet.avro.AvroParquetOutputFormat
//import org.apache.parquet.hadoop.util.ContextUtil
//import org.apache.parquet.hadoop.metadata.CompressionCodecName
// NOTE: Currently Parquet 1.8.1 is not compatible with Spark 1.5.1
// At the current time, we use the old Parquet 1.6.0 for uploading data to HDFS
import parquet.hadoop.ParquetOutputFormat
import parquet.avro.AvroParquetOutputFormat
import parquet.hadoop.util.ContextUtil
import parquet.hadoop.metadata.CompressionCodecName

import java.util.logging.{Level, Logger}

// batchedLineNum: the number of reads processed each time
class FASTQLocalFileLoader(batchedLineNum: Int) {
  var isEOF = false
  var ioWaitingTime = 0

  /**
    *  Read the FASTQ file from a local directory 
    *  This function reads only partial FASTQs and should be called several times to read the whole FASTQ file
    *  
    *  @param reader the Java BufferedReader object to read a file line by line
    *  @param sc the spark context
    *  @param batchedNum the number of lines to read per batch
    *  @param filePartitionNum the number of partitions in HDFS of this batch. We suggest to set this number equal to the number of core in the cluster.
    */
  def batchedRDDReader(reader: BufferedReader, sc: SparkContext, batchedNum: Int, filePartitionNum: Int): Vector[FASTQRecord] = {
    val charset = Charset.forName("ASCII")
    val encoder = charset.newEncoder()
    var records: Vector[FASTQRecord] = scala.collection.immutable.Vector.empty
    var lineNum = 0      
    var isBreak = false

    while(lineNum < batchedNum && !isBreak) {
      val line = reader.readLine()
      if(line != null) {
        val lineFields = line.split(" ")
        var tmpStr = new String

        if(lineFields.length <= 0) {
          println("Error: Input format not handled")
          System.exit(1);          
        }

        val n = lineFields(0).size
        if(lineFields(0).charAt(0) == '@') {
          if(lineFields(0).substring(n-2).equals("/1") || lineFields(0).substring(n-2).equals("/2"))
            tmpStr = lineFields(0).substring(1, n-2)
          else
            tmpStr = lineFields(0).substring(1)
        }
        else {
          if(lineFields(0).substring(n-2).equals("/1") || lineFields(0).substring(n-2).equals("/2"))
            tmpStr = lineFields(0).dropRight(2)
          else
            tmpStr = lineFields(0)
        }
        val name = encoder.encode( CharBuffer.wrap(tmpStr) )
        var comment: String = ""
        if(lineFields.length >= 2) {
          var i: Int = 1
          while(i < lineFields.length - 1) {
            comment += (lineFields(i) + " ")
            i += 1
          }
          comment += lineFields(lineFields.length - 1)
        }
        val seqString = reader.readLine()
        val seqLength = seqString.size
        val seq = encoder.encode( CharBuffer.wrap(seqString) )
        // read out the third line
        reader.readLine()
        val quality = encoder.encode( CharBuffer.wrap(reader.readLine()) )
        if(lineFields.length >= 2) {
          val record = new FASTQRecord(name, seq, quality, seqLength, encoder.encode( CharBuffer.wrap(comment) ))
          records = records :+ record
        }
        else {
          val record = new FASTQRecord(name, seq, quality, seqLength, encoder.encode( CharBuffer.wrap("") ))
          records = records :+ record
        }

        lineNum += 4
      }
      else {
        isEOF = true
        isBreak = true
      }
    }

    records
   }   


  /**
    *  Read the FASTQ file from the local file system and store it in HDFS
    *  The FASTQ is encoded and compressed in the Parquet+Avro format in HDFS 
    *  Note that there will be several directories since the local large FASTQ file is read and stored in HDFS with several batches
    *
    *  @param sc the spark context
    *  @param inFile the input FASTQ file in the local file system
    *  @param outFileRootPath the root path of the output FASTQ files in HDFS. 
    *  @param filePartitionNum the number of partitions in HDFS of this batch. We suggest to set this number equal to the number of core in the cluster.
    */
  def storeFASTQInHDFS(sc: SparkContext, inFile: String, outFileRootPath: String, filePartitionNum: Int) {
    val conf = new Configuration
    val fs = FileSystem.get(conf)
    val path = new Path(inFile)
    var reader: BufferedReader = null
    if (fs.exists(path)) {
      reader = new BufferedReader(new InputStreamReader(fs.open(path)))
    }
    else {
      reader = new BufferedReader(new FileReader(inFile)) //file reader
    }

    val parquetHadoopLogger = Logger.getLogger("parquet.hadoop")
    parquetHadoopLogger.setLevel(Level.SEVERE)

    var i: Int = 0

    while(!isEOF) {
      val serializedRecords = batchedRDDReader(reader, sc, batchedLineNum, filePartitionNum).map(new SerializableFASTQRecord(_))
      if(serializedRecords.length > 0) {
        val pairRDD = sc.parallelize(serializedRecords, filePartitionNum).map(rec => (null, rec))
        val job = new Job(pairRDD.context.hadoopConfiguration)

        // Configure the ParquetOutputFormat to use Avro as the serialization format
        //ParquetOutputFormat.setCompression(job, CompressionCodecName.GZIP)
        ParquetOutputFormat.setCompression(job, CompressionCodecName.UNCOMPRESSED)
        ParquetOutputFormat.setEnableDictionary(job, true)
        ParquetOutputFormat.setBlockSize(job, 128 * 1024 * 1024)
        ParquetOutputFormat.setPageSize(job, 1 * 1024 * 1024)
      
        // Pass the Avro Schema
        AvroParquetOutputFormat.setSchema(job, cs.ucla.edu.avro.fastq.FASTQRecord.SCHEMA$)
        // Save the RDD to a Parquet file in our temporary output directory
        val outputPath = outFileRootPath + "/"  + i.toString();
        //pairRDD.saveAsNewAPIHadoopFile(outputPath, classOf[Void], classOf[FASTQRecord], classOf[AvroParquetOutputFormat[FASTQRecord]], ContextUtil.getConfiguration(job))
        pairRDD.saveAsNewAPIHadoopFile(outputPath, classOf[Void], classOf[FASTQRecord], classOf[AvroParquetOutputFormat], ContextUtil.getConfiguration(job))

        i += 1
      }
    }
  }


  /**
    *  Read the FASTQ files (pair-end, 2 files) from a local directory 
    *  This function reads only a batch of FASTQs and should be called several times to read the whole FASTQ files
    *  
    *  @param reader1 the Java BufferedReader object to read a file line by line (on one end of the read)
    *  @param reader2 the Java BufferedReader object to read a file line by line (on the other end of the read)
    *  @param sc the spark context
    *  @param batchedNum the number of lines to read per batch
    *  @param filePartitionNum the number of partitions in HDFS of this batch. We suggest to set this number equal to the number of core in the cluster.
    */
  def batchedPairEndRDDReader(reader1: BufferedReader, reader2: BufferedReader, sc: SparkContext, batchedNum: Int, filePartitionNum: Int): Vector[PairEndFASTQRecord] = {
    val charset = Charset.forName("ASCII")
    val encoder = charset.newEncoder()
    var records: Vector[PairEndFASTQRecord] = scala.collection.immutable.Vector.empty
    var lineNum = 0      
    var isBreak = false

    while(lineNum < batchedNum && !isBreak) {
      var line = reader1.readLine()
      if(line != null) {
        val lineFields = line.split(" ")
        var pairEndRecord = new PairEndFASTQRecord        
        var tmpStr = new String
       
        if(lineFields.length <= 0) {
          println("Error: Input format not handled")
          System.exit(1);          
        }

        val n = lineFields(0).size
        if(lineFields(0).charAt(0) == '@') {
          if(lineFields(0).substring(n-2).equals("/1") || lineFields(0).substring(n-2).equals("/2")) 
            tmpStr = lineFields(0).substring(1, n-2)
          else
            tmpStr = lineFields(0).substring(1)
        }
        else {
          if(lineFields(0).substring(n-2).equals("/1") || lineFields(0).substring(n-2).equals("/2"))
            tmpStr = lineFields(0).dropRight(2)
          else
            tmpStr = lineFields(0)
        }
        val name = encoder.encode( CharBuffer.wrap(tmpStr) )
        var comment: String = ""
        if(lineFields.length >= 2) {
          var i: Int = 1
          while(i < lineFields.length - 1) {
            comment += (lineFields(i) + " ")
            i += 1
          }
          comment += lineFields(lineFields.length - 1)
        }
        val seqString = reader1.readLine()
        val seqLength = seqString.size
        val seq = encoder.encode( CharBuffer.wrap(seqString) )
        // read out the third line
        reader1.readLine()
        val quality = encoder.encode( CharBuffer.wrap(reader1.readLine()) )
        if(lineFields.length >= 2) {
          val record = new FASTQRecord(name, seq, quality, seqLength, encoder.encode( CharBuffer.wrap(comment) ))
          pairEndRecord.setSeq0(new SerializableFASTQRecord(record))
        }
        else {
          val record = new FASTQRecord(name, seq, quality, seqLength, encoder.encode( CharBuffer.wrap("") ))
          pairEndRecord.setSeq0(new SerializableFASTQRecord(record))
        }

        line = reader2.readLine

        if(line == null) {
          println("Error: the number of two FASTQ files are different")
          System.exit(1)
        }
        else {
          val lineFields = line.split(" ")
          if(lineFields.length <= 0) {
            println("Error: Input format not handled")
            System.exit(1);          
          }

          val n = lineFields(0).size
          if(lineFields(0).charAt(0) == '@') {
            if(lineFields(0).substring(n-2).equals("/1") || lineFields(0).substring(n-2).equals("/2"))
              tmpStr = lineFields(0).substring(1, n-2)
            else
              tmpStr = lineFields(0).substring(1)
          }
          else {
            if(lineFields(0).substring(n-2).equals("/1") || lineFields(0).substring(n-2).equals("/2"))
              tmpStr = lineFields(0).dropRight(2)
            else
              tmpStr = lineFields(0)
          }
          val name = encoder.encode( CharBuffer.wrap(tmpStr) )
          var comment: String = ""
          if(lineFields.length >= 2) {
            var i: Int = 1
            while(i < lineFields.length - 1) {
              comment += (lineFields(i) + " ")
              i += 1
            }
            comment += lineFields(lineFields.length - 1)
          }
          val seqString = reader2.readLine()
          val seqLength = seqString.size
          val seq = encoder.encode( CharBuffer.wrap(seqString) )
          // read out the third line
          reader2.readLine()
          val quality = encoder.encode( CharBuffer.wrap(reader2.readLine()) )
          if(lineFields.length >= 2) {
            val record = new FASTQRecord(name, seq, quality, seqLength, encoder.encode( CharBuffer.wrap(comment) ))
            pairEndRecord.setSeq1(new SerializableFASTQRecord(record))
          }
          else {
            val record = new FASTQRecord(name, seq, quality, seqLength, encoder.encode( CharBuffer.wrap("") ))
            pairEndRecord.setSeq1(new SerializableFASTQRecord(record))
          }
        }
 
        records = records :+ pairEndRecord
        lineNum += 8
      }
      else {
        isEOF = true
        isBreak = true
      }
    }

    records
  }   


  /**
    *  Read the FASTQ file and place reads in an array
    *  
    *  @param batchedLineNum the number of lines processed each time (equals to # of reads * 4)
    *  @param reader the Java BufferedReader object to read a file line by line
    *  @return a ParArray that store these reads
    */
  def bufferedReadFASTQ(batchedLineNum: Int, reader: BufferedReader) : ParArray[RawRead] = {
    val parArraySize = batchedLineNum / 4
    val rawReads: ParArray[RawRead] = new ParArray(parArraySize)

    var lineNum = 0
    var readIdx = 0
    var isBreak = false

    while(lineNum < batchedLineNum && !isBreak) {
      var line = reader.readLine
      if(line != null) {
        val read = new RawRead
        read.name = line
        read.seq = reader.readLine
        read.description = reader.readLine
        read.qual = reader.readLine
        rawReads(readIdx) = read
        lineNum += 4
        readIdx += 1
      }
      else {
        isEOF = true
        isBreak = true
      }
    }

    if(!isBreak) {
      println("Raw read #: " + rawReads.size)
      rawReads
    }
    else {
      val remainingRawReads: ParArray[RawRead] = new ParArray(readIdx)
      println("Remaining raw read #: " + remainingRawReads.size)
      var i = 0
      while(i < readIdx) {
        remainingRawReads(i) = rawReads(i)
        i += 1
      }
      remainingRawReads
    }
  }


  /**
    *  Transform raw read to read record
    *
    *  @param rawRead the raw read store in the String format
    *  @return a read record
    */
  def RawRead2FASTQRecord(rawRead: RawRead): SerializableFASTQRecord = {
    val charset = Charset.forName("ASCII")
    val encoder = charset.newEncoder
    val lineFields = rawRead.name.split(" ")
    var tmpStr = new String
       
    if(lineFields.length <= 0) {
      println("Error: Input format not handled")
      System.exit(1);          
    }

    val n = lineFields(0).size
    if(lineFields(0).charAt(0) == '@') {
      if(lineFields(0).substring(n-2).equals("/1") || lineFields(0).substring(n-2).equals("/2")) 
        tmpStr = lineFields(0).substring(1, n-2)
      else
        tmpStr = lineFields(0).substring(1)
    }
    else {
      if(lineFields(0).substring(n-2).equals("/1") || lineFields(0).substring(n-2).equals("/2"))
        tmpStr = lineFields(0).dropRight(2)
      else
        tmpStr = lineFields(0)
    }
    val name = encoder.encode( CharBuffer.wrap(tmpStr) )
    var comment: String = ""
    if(lineFields.length >= 2) {
      var i: Int = 1
      while(i < lineFields.length - 1) {
        comment += (lineFields(i) + " ")
        i += 1
      }
      comment += lineFields(lineFields.length - 1)
    }
    val seqLength = rawRead.seq.size
    val seq = encoder.encode( CharBuffer.wrap(rawRead.seq) )
    val quality = encoder.encode( CharBuffer.wrap(rawRead.qual) )
    
    if(lineFields.length >= 2) {
      val record = new FASTQRecord(name, seq, quality, seqLength, encoder.encode( CharBuffer.wrap(comment) ))
      val serializedRecord = new SerializableFASTQRecord(record)
      serializedRecord
    }
    else {
      val record = new FASTQRecord(name, seq, quality, seqLength, encoder.encode( CharBuffer.wrap("") ))
      val serializedRecord = new SerializableFASTQRecord(record)
      serializedRecord
    }
  }


  /**
    *  Read the FASTQ files (pair-end, 2 files) from a local directory in a PARALLEL way
    *  This function reads only a batch of FASTQs and should be called several times to read the whole FASTQ files
    *  
    *  @param reader1 the Java BufferedReader object to read a file line by line (on one end of the read)
    *  @param reader2 the Java BufferedReader object to read a file line by line (on the other end of the read)
    *  @param batchedNum the number of lines to read per batch
    */
  def parallelBatchedPairEndRDDReader(reader1: BufferedReader, reader2: BufferedReader, batchedNum: Int): Vector[PairEndFASTQRecord] = {
    var records: Vector[PairEndFASTQRecord] = scala.collection.immutable.Vector.empty
    val readEnds0 = bufferedReadFASTQ(batchedNum / 2, reader1)
    val readEnds1 = bufferedReadFASTQ(batchedNum / 2, reader2)
    val fastqRecords0 = readEnds0.map( RawRead2FASTQRecord(_) )
    val fastqRecords1 = readEnds1.map( RawRead2FASTQRecord(_) )

    if(fastqRecords0.size != fastqRecords1.size) {
      println("Error: the number of two FASTQ files are different")
      System.exit(1)
    }

    var i: Int = 0
    while(i < fastqRecords0.size) {
      val pairEndRecord = new PairEndFASTQRecord
      pairEndRecord.setSeq0(fastqRecords0(i))       
      pairEndRecord.setSeq1(fastqRecords1(i))       
      records = records :+ pairEndRecord
      i += 1
    }    

    records
  }   


  /**
    *  Read the FASTQ file from the local file system and store it in HDFS
    *  The FASTQ is encoded and compressed in the Parquet+Avro format in HDFS 
    *  Note that there will be several directories since the local large FASTQ file is read and stored in HDFS with several batches
    *
    *  @param sc the spark context
    *  @param inFile1 the input FASTQ file in the local file system (on one end of the read)
    *  @param inFile2 the input FASTQ file in the local file system (on the other end of the read)
    *  @param outFileRootPath the root path of the output FASTQ files in HDFS. 
    *  @param filePartitionNum the number of partitions in HDFS of this batch. We suggest to set this number equal to the number of core in the cluster.
    */
  def storePairEndFASTQInHDFS(sc: SparkContext, inFile1: String, inFile2: String, outFileRootPath: String, filePartitionNum: Int) {
    val reader1 = new BufferedReader(new FileReader(inFile1))
    val reader2 = new BufferedReader(new FileReader(inFile2))

    val parquetHadoopLogger = Logger.getLogger("parquet.hadoop")
    parquetHadoopLogger.setLevel(Level.SEVERE)

    var i: Int = 0

    var isHDFSWriteDone: Boolean = true   // a done signal for writing data to HDFS

    while(!isEOF) {
      //val serializedRecords = batchedPairEndRDDReader(reader1, reader2, sc, batchedLineNum, filePartitionNum).map(new SerializablePairEndFASTQRecord(_)) // old implementation
      val serializedRecords = parallelBatchedPairEndRDDReader(reader1, reader2, batchedLineNum).map(new SerializablePairEndFASTQRecord(_))
      if(serializedRecords.length > 0) {

        println("[DEBUG] Main Thread, Before while loop: isHDFSWriteDone = " + isHDFSWriteDone)
        while(!isHDFSWriteDone) {
          try {
            println("Waiting for I/O")
            ioWaitingTime += 1
            Thread.sleep(1000)   // sleep for one second
          } catch {
            case e: InterruptedException => Thread.currentThread.interrupt
          }
        }
        println("[DEBUG] Main Thread, After while loop: isHDFSWriteDone = " + isHDFSWriteDone)
        this.synchronized {
          isHDFSWriteDone = false
        }
        println("[DEBUG] Main Thread, Final value: isHDFSWriteDone = " + isHDFSWriteDone)

        val f: Future[Int] = Future {
          val pairRDD = sc.parallelize(serializedRecords, filePartitionNum).map(rec => (null, rec))
          val job = new Job(pairRDD.context.hadoopConfiguration)

          // Configure the ParquetOutputFormat to use Avro as the serialization format
          //ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport[PairEndFASTQRecord]])
          //ParquetOutputFormat.setCompression(job, CompressionCodecName.GZIP)
          ParquetOutputFormat.setCompression(job, CompressionCodecName.UNCOMPRESSED)
          ParquetOutputFormat.setEnableDictionary(job, true)
          ParquetOutputFormat.setBlockSize(job, 128 * 1024 * 1024)
          ParquetOutputFormat.setPageSize(job, 1 * 1024 * 1024)
      
          // Pass the Avro Schema
          AvroParquetOutputFormat.setSchema(job, cs.ucla.edu.avro.fastq.PairEndFASTQRecord.SCHEMA$)
          // Save the RDD to a Parquet file in our temporary output directory
          val outputPath = outFileRootPath + "/"  + i.toString();
          pairRDD.saveAsNewAPIHadoopFile(outputPath, classOf[Void], classOf[PairEndFASTQRecord], classOf[AvroParquetOutputFormat], ContextUtil.getConfiguration(job))
          //pairRDD.saveAsNewAPIHadoopFile(outputPath, classOf[Void], classOf[PairEndFASTQRecord], classOf[AvroParquetOutputFormat[PairEndFASTQRecord]], ContextUtil.getConfiguration(job))
          i += 1
          1
        }

        f onComplete {
          case Success(s) => {
            println("[DEBUG] Forked thread, Before: isHDFSWriteDone = " + isHDFSWriteDone)
            println("Successfully write the FASTQ file partitions to HDFS: " + s)
            this.synchronized {
              isHDFSWriteDone = true
            }
            println("[DEBUG] Forked thread, After: isHDFSWriteDone = " + isHDFSWriteDone)
          }
          case Failure(f) => println("An error has occured: " + f.getMessage)
        }

      }
    }

    println("[DEBUG] Main Thread (Final iteration), Before while loop: isHDFSWriteDone = " + isHDFSWriteDone)
    while(!isHDFSWriteDone) {
      try {
        println("Waiting for I/O")
        Thread.sleep(1000)   // sleep for one second
      } catch {
        case e: InterruptedException => Thread.currentThread.interrupt
      }
    }
    println("[DEBUG] Main Thread (Final iteration), After while loop: isHDFSWriteDone = " + isHDFSWriteDone)
    this.synchronized {
      isHDFSWriteDone = false
    }
    println("[DEBUG] Main Thread (Final iteration), Final value: isHDFSWriteDone = " + isHDFSWriteDone)
    
  }

}


package cs.ucla.edu.bwaspark.fastq

import java.io.{File, FileReader, BufferedReader, IOException, FileNotFoundException}
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{Charset, CharsetEncoder, CharacterCodingException}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.util.control.Breaks._
import scala.List

import cs.ucla.edu.avro.fastq._

import parquet.hadoop.ParquetOutputFormat
import parquet.avro.AvroParquetOutputFormat
import org.apache.hadoop.mapreduce.Job
import parquet.hadoop.util.ContextUtil
import parquet.hadoop.metadata.CompressionCodecName

import java.util.logging.{Level, Logger}

// batchedLineNum: the number of reads processed each time
class FASTQLocalFileLoader(batchedLineNum: Int) {
   var isEOF = false

   /**
     *  Read the FASTQ file from a local directory 
     *  This function reads only partial FASTQs and should be called several times to read the whole FASTQ file
     *  
     *  @param reader the Java BufferedReader object to read a file line by line
     *  @param sc the spark context
     *  @param batchedNum the number of lines to read per batch
     */
   def batchedRDDReader(reader: BufferedReader, sc: SparkContext, batchedNum: Int): RDD[(Null, SerializableFASTQRecord)] = {
      val charset = Charset.forName("ASCII")
      val encoder = charset.newEncoder()
      var records: Vector[FASTQRecord] = scala.collection.immutable.Vector.empty
      var lineNum = 0      
      var isBreak = false

      while(lineNum < batchedNum && !isBreak) {
         val line = reader.readLine()
         if(line != null) {
            //val lineFields = line.split("""\s""")
            val lineFields = line.split(" ")
               
            if(lineFields.length == 1) {
               val name = encoder.encode( CharBuffer.wrap(lineFields(0)) ); 
               val seqString = reader.readLine()
               val seqLength = seqString.size
               val seq = encoder.encode( CharBuffer.wrap(seqString) )
               // read out the third line
               reader.readLine()
               val quality = encoder.encode( CharBuffer.wrap(reader.readLine()) )
               val record = new FASTQRecord(name, seq, quality, seqLength, encoder.encode( CharBuffer.wrap("") ))
               records = records :+ record
            } else if(lineFields.length == 2) {
               val name = encoder.encode( CharBuffer.wrap(lineFields(0)) );
               val comment = encoder.encode( CharBuffer.wrap(lineFields(1)) );
               val seqString = reader.readLine()
               val seqLength = seqString.size
               val seq = encoder.encode( CharBuffer.wrap(seqString) )
               // read out the third line
               reader.readLine()
               val quality = encoder.encode( CharBuffer.wrap(reader.readLine()) )
               val record = new FASTQRecord(name, seq, quality, seqLength, comment)
               records = records :+ record
            }
            else
               println("Error: Input format not handled")

            lineNum += 4
         }
         else {
            isEOF = true
            isBreak = true
         }
      }


      val serializedRecords = records.map(new SerializableFASTQRecord(_))
      val rdd = sc.parallelize(serializedRecords)
      val pairRDD = rdd.map(rec => (null, rec))
      pairRDD
   }   


   /**
     *  Read the FASTQ file from the local file system and store it in HDFS
     *  The FASTQ is encoded and compressed in the Parquet+Avro format in HDFS 
     *
     *  @param sc the spark context
     *  @param inFile the input FASTQ file in the local file system
     *  @param outFileRootPath the root path of the output FASTQ files in HDFS. 
     *    Note that there will be several directories since the local large FASTQ file is read and stored in HDFS with several batches
     */
   def storeFASTQInHDFS(sc: SparkContext, inFile: String, outFileRootPath: String) {
      val reader = new BufferedReader(new FileReader(inFile))

      val parquetHadoopLogger = Logger.getLogger("parquet.hadoop")
      parquetHadoopLogger.setLevel(Level.SEVERE)

      var i: Int = 0

      while(!isEOF) {
         val pairRDD = batchedRDDReader(reader, sc, batchedLineNum)
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
         pairRDD.saveAsNewAPIHadoopFile(outputPath, classOf[Void], classOf[FASTQRecord], classOf[AvroParquetOutputFormat], ContextUtil.getConfiguration(job))
         i += 1
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
     */
   def batchedPairEndRDDReader(reader1: BufferedReader, reader2: BufferedReader, sc: SparkContext, batchedNum: Int): RDD[(Null, SerializablePairEndFASTQRecord)] = {
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
       
            if(lineFields.length == 1) {
               val name = encoder.encode( CharBuffer.wrap(lineFields(0)) ); 
               val seqString = reader1.readLine()
               val seqLength = seqString.size
               val seq = encoder.encode( CharBuffer.wrap(seqString) )
               // read out the third line
               reader1.readLine()
               val quality = encoder.encode( CharBuffer.wrap(reader1.readLine()) )
               val record = new FASTQRecord(name, seq, quality, seqLength, encoder.encode( CharBuffer.wrap("") ))
               pairEndRecord.setSeq0(new SerializableFASTQRecord(record))
            } else if(lineFields.length == 2) {
               val name = encoder.encode( CharBuffer.wrap(lineFields(0)) );
               val comment = encoder.encode( CharBuffer.wrap(lineFields(1)) );
               val seqString = reader1.readLine()
               val seqLength = seqString.size
               val seq = encoder.encode( CharBuffer.wrap(seqString) )
               // read out the third line
               reader1.readLine()
               val quality = encoder.encode( CharBuffer.wrap(reader1.readLine()) )
               val record = new FASTQRecord(name, seq, quality, seqLength, comment)
               pairEndRecord.setSeq0(new SerializableFASTQRecord(record))
            }
            else
               println("Error: Input format not handled")

            line = reader2.readLine

            if(line == null) println("Error: the number of two FASTQ files are different")
            else {
                 
               val lineFields = line.split(" ")
                
               if(lineFields.length == 1) {
                  val name = encoder.encode( CharBuffer.wrap(lineFields(0)) ); 
                  val seqString = reader2.readLine()
                  val seqLength = seqString.size
                  val seq = encoder.encode( CharBuffer.wrap(seqString) )
                  // read out the third line
                  reader2.readLine()
                  val quality = encoder.encode( CharBuffer.wrap(reader2.readLine()) )
                  val record = new FASTQRecord(name, seq, quality, seqLength, encoder.encode( CharBuffer.wrap("") ))
                  pairEndRecord.setSeq1(new SerializableFASTQRecord(record))
               } else if(lineFields.length == 2) {
                  val name = encoder.encode( CharBuffer.wrap(lineFields(0)) );
                  val comment = encoder.encode( CharBuffer.wrap(lineFields(1)) );
                  val seqString = reader2.readLine()
                  val seqLength = seqString.size
                  val seq = encoder.encode( CharBuffer.wrap(seqString) )
                  // read out the third line
                  reader2.readLine()
                  val quality = encoder.encode( CharBuffer.wrap(reader2.readLine()) )
                  val record = new FASTQRecord(name, seq, quality, seqLength, comment)
                  pairEndRecord.setSeq1(new SerializableFASTQRecord(record))
               }
               else
                  println("Error: Input format not handled")
            }
 
            records = records :+ pairEndRecord
            lineNum += 8
         }
         else {
            isEOF = true
            isBreak = true
         }
      }


      val serializedRecords = records.map(new SerializablePairEndFASTQRecord(_))
      val rdd = sc.parallelize(serializedRecords)
      val pairRDD = rdd.map(rec => (null, rec))
      pairRDD
   }   


   /**
     *  Read the FASTQ file from the local file system and store it in HDFS
     *  The FASTQ is encoded and compressed in the Parquet+Avro format in HDFS 
     *
     *  @param sc the spark context
     *  @param inFile1 the input FASTQ file in the local file system (on one end of the read)
     *  @param inFile2 the input FASTQ file in the local file system (on the other end of the read)
     *  @param outFileRootPath the root path of the output FASTQ files in HDFS. 
     *    Note that there will be several directories since the local large FASTQ file is read and stored in HDFS with several batches
     */
   def storePairEndFASTQInHDFS(sc: SparkContext, inFile1: String, inFile2: String, outFileRootPath: String) {
      val reader1 = new BufferedReader(new FileReader(inFile1))
      val reader2 = new BufferedReader(new FileReader(inFile2))

      val parquetHadoopLogger = Logger.getLogger("parquet.hadoop")
      parquetHadoopLogger.setLevel(Level.SEVERE)

      var i: Int = 0

      while(!isEOF) {
         val pairRDD = batchedPairEndRDDReader(reader1, reader2, sc, batchedLineNum)
         val job = new Job(pairRDD.context.hadoopConfiguration)

         // Configure the ParquetOutputFormat to use Avro as the serialization format
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
         i += 1
      }
   }

}


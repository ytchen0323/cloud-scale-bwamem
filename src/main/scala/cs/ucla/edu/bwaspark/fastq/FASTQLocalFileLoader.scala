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
      var records = List[FASTQRecord]()
      var lineNum = 0      

      breakable {
         while(lineNum < batchedNum) {
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
                  records = record :: records
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
                  records = record :: records
               }
               else
                  println("Error: Input format not handled")

               lineNum += 4
            }
            else {
               isEOF = true
               break;
            }
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

}


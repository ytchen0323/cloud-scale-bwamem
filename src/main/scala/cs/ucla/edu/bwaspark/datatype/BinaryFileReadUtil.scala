package cs.ucla.edu.bwaspark.datatype

import java.io.{FileInputStream, IOException}
import java.nio.{ByteBuffer}
import java.nio.file.{Files, Path, Paths}
import java.nio.channels.FileChannel
import java.nio.ByteOrder

import scala.util.control.Breaks._
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

object BinaryFileReadUtil {
  val readBufSize = 0x80000  

  /**
    *  Read an input binary file with Long type values until the end of the file
    *
    *  @param fc the given file channel to read the file
    *  @param arraySize the output Long array size
    *  @param startIdx the starting index of the array to fill the data
    */
  def readLongArray(fc: FileChannel, arraySize: Int, startIdx: Int): Array[Long] = {
    val buf = ByteBuffer.allocate(readBufSize)
    buf.order(ByteOrder.nativeOrder)
    var ret = 0
    var outputArray = new Array[Long](arraySize)
    var idx = startIdx

    while(ret >= 0) {
      ret = fc.read(buf)
      buf.flip
     
      // Fill the data from buf
      while(buf.hasRemaining) {
        val piece = buf.getLong
        outputArray(idx) = piece
        idx += 1
      }

      buf.rewind
    }

    outputArray
  }

  //HDFS version of readLongArray
  def readLongArray(fc: FSDataInputStream, arraySize: Int, startIdx: Int): Array[Long] = {
    val bytes = new Array[Byte](readBufSize)
    var ret = 0
    var outputArray = new Array[Long](arraySize)
    var idx = startIdx

    ret = fc.read(bytes)
    while(ret >= 0) {
      assert (ret > 0)

      val buf = ByteBuffer.wrap(bytes, 0, ret)
      buf.order(ByteOrder.nativeOrder)
     
      // Fill the data from buf
      while(buf.hasRemaining) {
        val piece = buf.getLong
        outputArray(idx) = piece
        idx += 1
      }

      ret = fc.read(bytes)
    }

    ////val buf = ByteBuffer.allocate(readBufSize)
    ////buf.order(ByteOrder.nativeOrder)
    ////var ret = 0
    ////var outputArray = new Array[Long](arraySize)
    ////var idx = startIdx

    ////while(ret >= 0) {
    ////  ret = fc.read(buf)
    ////  buf.flip
    //// 
    ////  // Fill the data from buf
    ////  while(buf.hasRemaining) {
    ////    val piece = buf.getLong
    ////    outputArray(idx) = piece
    ////    idx += 1
    ////  }

    ////  buf.rewind
    ////}

    outputArray
  }

  
  /**
    *  Read an input binary file with Int type values until the end of the file
    *
    *  @param fc the given file channel to read the file
    *  @param arraySize the output Int array size
    *  @param startIdx the starting index of the array to fill the data
    */
  def readIntArray(fc: FileChannel, arraySize: Int, startIdx: Int): Array[Int] = {
    val buf = ByteBuffer.allocate(readBufSize)
    buf.order(ByteOrder.nativeOrder)
    var ret = 0
    var outputArray = new Array[Int](arraySize)
    var idx = startIdx

    while(ret >= 0) {
      ret = fc.read(buf)
      buf.flip
     
      // Fill the data from buf
      while(buf.hasRemaining) {
        val piece = buf.getInt
        outputArray(idx) = piece
        idx += 1
      }

      buf.rewind
    }

    outputArray
  }

  //HDFS version of readIntArray
  def readIntArray(fc: FSDataInputStream, arraySize: Int, startIdx: Int): Array[Int] = {
    val bytes = new Array[Byte](readBufSize)
    var ret = 0
    var outputArray = new Array[Int](arraySize)
    var idx = startIdx

    ret = fc.read(bytes)
    while(ret >= 0) {
      assert (ret > 0)

      val buf = ByteBuffer.wrap(bytes, 0, ret)
      buf.order(ByteOrder.nativeOrder)
     
      // Fill the data from buf
      while(buf.hasRemaining) {
        val piece = buf.getInt
        outputArray(idx) = piece
        idx += 1
      }

      //buf.rewind
      ret = fc.read(bytes)
    }
    ////val buf = ByteBuffer.allocate(readBufSize)
    ////buf.order(ByteOrder.nativeOrder)
    ////var ret = 0
    ////var outputArray = new Array[Int](arraySize)
    ////var idx = startIdx

    ////while(ret >= 0) {
    ////  ret = fc.read(buf)
    ////  buf.flip
    //// 
    ////  // Fill the data from buf
    ////  while(buf.hasRemaining) {
    ////    val piece = buf.getInt
    ////    outputArray(idx) = piece
    ////    idx += 1
    ////  }

    ////  buf.rewind
    ////}

    outputArray
  }

  
  /**
    *  Read an input binary file with Byte type values 
    *  This function does not assume to read the binary file to the end.
    *
    *  @param fc the given file channel to read the file
    *  @param arraySize the output Byte array size
    *  @param startIdx the starting index of the array to fill the data
    */
  def readByteArray(fc: FileChannel, arraySize: Int, startIdx: Int): Array[Byte] = {
    val buf = ByteBuffer.allocate(readBufSize)
    buf.order(ByteOrder.nativeOrder)
    var ret = 0
    var outputArray = new Array[Byte](arraySize)
    var idx = startIdx
    var reachSizeLimit = false

    while(ret >= 0 && !reachSizeLimit) {
      ret = fc.read(buf)
      buf.flip
     
      // Fill the data from buf
      while(buf.hasRemaining && !reachSizeLimit) {
        val piece = buf.get
        outputArray(idx) = piece
        idx += 1

        if(idx >= arraySize) 
          reachSizeLimit = true
      }

      buf.rewind
    }

    outputArray
  }

  //HDFS version of readByteArray
  def readByteArray(fc: FSDataInputStream, arraySize: Int, startIdx: Int): Array[Byte] = {
    var ret = 0
    var outputArray = new Array[Byte](arraySize)

    ret = fc.read(outputArray)
    assert (ret == arraySize)

    ////val buf = ByteBuffer.allocate(readBufSize)
    ////buf.order(ByteOrder.nativeOrder)
    ////var ret = 0
    ////var outputArray = new Array[Byte](arraySize)
    ////var idx = startIdx
    ////var reachSizeLimit = false

    ////while(ret >= 0 && !reachSizeLimit) {
    ////  ret = fc.read(buf)
    ////  buf.flip
    //// 
    ////  // Fill the data from buf
    ////  while(buf.hasRemaining && !reachSizeLimit) {
    ////    val piece = buf.get
    ////    outputArray(idx) = piece
    ////    idx += 1

    ////    if(idx >= arraySize) 
    ////      reachSizeLimit = true
    ////  }

    ////  buf.rewind
    ////}

    outputArray
  }


  /**
    *  Read a single Long value from a binary file
    *
    *  @param fc the given file channel to read the file
    */
  def readLong(fc: FileChannel): Long = {
    val buf = ByteBuffer.allocate(8)
    buf.order(ByteOrder.nativeOrder)
    fc.read(buf)
    buf.flip
    buf.getLong
  }

  //HDFS Version of readLong
  def readLong(fc: FSDataInputStream): Long = {
    var bytes = new Array[Byte](8)
    fc.read(bytes, 0, 8)
    val buf = ByteBuffer.wrap(bytes)
    buf.order(ByteOrder.nativeOrder)
    //val buf = ByteBuffer.allocate(8)
    //buf.order(ByteOrder.nativeOrder)
    //fc.read(buf)
    //buf.flip
    buf.getLong
  }


  /**
    *  Read a single Int value from a binary file
    *
    *  @param fc the given file channel to read the file
    */
  def readInt(fc: FileChannel): Int = {
    val buf = ByteBuffer.allocate(4)
    buf.order(ByteOrder.nativeOrder)
    fc.read(buf)
    buf.flip
    buf.getInt
  }

  //HDFS version of readInt
  def readInt(fc: FSDataInputStream): Int = {
    var bytes = new Array[Byte](4)
    fc.read(bytes, 0, 4)
    val buf = ByteBuffer.wrap(bytes)
    buf.order(ByteOrder.nativeOrder)
    //val buf = ByteBuffer.allocate(4)
    //buf.order(ByteOrder.nativeOrder)
    //fc.read(buf)
    //buf.flip
    buf.getInt
  }

}


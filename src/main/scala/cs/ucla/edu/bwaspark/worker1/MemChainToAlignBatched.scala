package cs.ucla.edu.bwaspark.worker1

import scala.collection.mutable.MutableList

import cs.ucla.edu.bwaspark.datatype._
import cs.ucla.edu.bwaspark.util.BNTSeqUtil._
import cs.ucla.edu.bwaspark.util.SWUtil._
import cs.ucla.edu.bwaspark.debug.DebugFlag._

import accUCLA.api._
import java.util._
import java.io.IOException
import java.io.ObjectOutputStream
import java.io.OutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.net.Socket
import java.net.InetAddress
import java.net.ServerSocket

// Used for read test input data
import java.io.{FileReader, BufferedReader}

// JNI function for SWExtend
import cs.ucla.edu.bwaspark.jni.SWExtendFPGAJNI


object MemChainToAlignBatched {
  val MAX_BAND_TRY = 2    
  val MARKED = -2
  // Use for FPGA Kernel
  val commonSize = 32
  val indivSize = 32
  val retValues = 8
  val FPGA_RET_PARAM_NUM = 5

  //Run DPs on FPGA
  def runOnFPGAJNI(taskNum: Int, //number of tasks
                   tasks: Array[ExtParam], // task array
                   results: Array[ExtRet] // result array
                  ) {
    def int2ByteArray(arr: Array[Byte], idx: Int, num: Int): Int = {
      arr(idx) = (num & 0xff).toByte
      arr(idx+1) = ((num >> 8) & 0xff).toByte
      arr(idx+2) = ((num >> 16) & 0xff).toByte
      arr(idx+3) = ((num >> 24) & 0xff).toByte
      idx+4
    }
    def short2ByteArray(arr: Array[Byte], idx: Int, num: Short): Int = {
      arr(idx) = (num & 0xff).toByte
      arr(idx+1) = ((num >> 8) & 0xff).toByte
      idx+2
    }

    val buf1Len = commonSize + indivSize*taskNum
    val buf1 = new Array[Byte](buf1Len)
    buf1(0) = (tasks(0).oDel.toByte)
    buf1(1) = (tasks(0).eDel.toByte)
    buf1(2) = (tasks(0).oIns.toByte)
    buf1(3) = (tasks(0).eIns.toByte)
    buf1(4) = (tasks(0).penClip5.toByte)
    buf1(5) = (tasks(0).penClip3.toByte)
    buf1(6) = (tasks(0).w.toByte)
    int2ByteArray(buf1, 8, taskNum) //8,9,10,11

    var i = 0
    var leftMaxIns = 0
    var leftMaxDel = 0
    var rightMaxIns = 0
    var rightMaxDel = 0
    var taskPos = buf1Len >> 2
    var buf1Idx = 32

    while (i < taskNum) {
      buf1Idx = short2ByteArray(buf1, buf1Idx, tasks(i).leftQlen.toShort)
      buf1Idx = short2ByteArray(buf1, buf1Idx, tasks(i).leftRlen.toShort)
      buf1Idx = short2ByteArray(buf1, buf1Idx, tasks(i).rightQlen.toShort)
      buf1Idx = short2ByteArray(buf1, buf1Idx, tasks(i).rightRlen.toShort)
      buf1Idx = int2ByteArray(buf1, buf1Idx, taskPos)
      taskPos += ((((tasks(i).leftQlen + tasks(i).leftRlen + tasks(i).rightQlen + tasks(i).rightRlen)+1)/2)+3)/4
      buf1Idx = short2ByteArray(buf1, buf1Idx, tasks(i).regScore.toShort)
      buf1Idx = short2ByteArray(buf1, buf1Idx, tasks(i).qBeg.toShort)
      buf1Idx = short2ByteArray(buf1, buf1Idx, tasks(i).h0.toShort)
      buf1Idx = short2ByteArray(buf1, buf1Idx, tasks(i).idx.toShort)
      leftMaxIns = ((tasks(i).leftQlen * tasks(i).mat.max + tasks(i).penClip5 - tasks(i).oIns).toDouble / tasks(i).eIns + 1).toInt
      leftMaxDel = ((tasks(i).leftQlen * tasks(i).mat.max + tasks(i).penClip5 - tasks(i).oDel).toDouble / tasks(i).eDel + 1).toInt
      rightMaxIns = ((tasks(i).rightQlen * tasks(i).mat.max + tasks(i).penClip3 - tasks(i).oIns).toDouble / tasks(i).eIns + 1).toInt
      rightMaxDel = ((tasks(i).rightQlen * tasks(i).mat.max + tasks(i).penClip3 - tasks(i).oDel).toDouble / tasks(i).eDel + 1).toInt
      buf1Idx = short2ByteArray(buf1, buf1Idx, leftMaxIns.toShort)
      buf1Idx = short2ByteArray(buf1, buf1Idx, leftMaxDel.toShort)
      buf1Idx = short2ByteArray(buf1, buf1Idx, rightMaxIns.toShort)
      buf1Idx = short2ByteArray(buf1, buf1Idx, rightMaxDel.toShort)
      buf1Idx = int2ByteArray(buf1, buf1Idx, tasks(i).idx)

      i = i+1
    }

    val buf2 = new Array[Byte]((taskPos<<2)-buf1Len)
    var buf2Idx = 0
    i = 0
    var j = 0
    var tmpIntVar = 0
    var counter8 = 0
    while (i < taskNum) {
      if (tasks(i).leftQlen > 0) {
        j = 0
        while (j < tasks(i).leftQlen) {
            counter8 = counter8 + 1
            tmpIntVar = tmpIntVar << 4 | (tasks(i).leftQs(j).toInt & 0x0F)
            if (counter8 % 8 == 0) buf2Idx = int2ByteArray(buf2, buf2Idx, tmpIntVar)
            j = j + 1
        }
      }
      if (tasks(i).rightQlen > 0) {
        j = 0
        while (j < tasks(i).rightQlen) {
            counter8 = counter8 + 1
            tmpIntVar = tmpIntVar << 4 | (tasks(i).rightQs(j).toInt & 0x0F)
            if (counter8 % 8 == 0) buf2Idx = int2ByteArray(buf2, buf2Idx, tmpIntVar)
            j = j + 1
        }
      }
      if (tasks(i).leftRlen > 0) {
        j = 0
        while (j < tasks(i).leftRlen) {
            counter8 = counter8 + 1
            tmpIntVar = tmpIntVar << 4 | (tasks(i).leftRs(j).toInt & 0x0F)
            if (counter8 % 8 == 0) buf2Idx = int2ByteArray(buf2, buf2Idx, tmpIntVar)
            j = j + 1
        }
      }
      if (tasks(i).rightRlen > 0) {
        j = 0
        while (j < tasks(i).rightRlen) {
            counter8 = counter8 + 1
            tmpIntVar = tmpIntVar << 4 | (tasks(i).rightRs(j).toInt & 0x0F)
            if (counter8 % 8 == 0) buf2Idx = int2ByteArray(buf2, buf2Idx, tmpIntVar)
            j = j + 1
        }
      }
      if (counter8 % 8 != 0) {
        while (counter8 % 8 != 0) {
            tmpIntVar = tmpIntVar << 4
            counter8 = counter8 + 1
        }
        buf2Idx = int2ByteArray(buf2, buf2Idx, tmpIntVar)
      }
      i = i + 1
    }

    val buf2Host = Array.concat(buf1, buf2)

    // JNI
    val jni = new SWExtendFPGAJNI
    val bufRet = jni.swExtendFPGAJNI(taskNum * FPGA_RET_PARAM_NUM * 2, buf2Host)  // taskNum * FPGA_RET_PARAM_NUM * 2 == NUM of short integers to be returned

    i = 0
    while (i < taskNum) {
      if (results(i) == null) results(i) = new ExtRet
      results(i).idx = ((bufRet(1+FPGA_RET_PARAM_NUM*2*i).toInt) << 16) | bufRet(0+FPGA_RET_PARAM_NUM*2*i).toInt
      results(i).qBeg = bufRet(2+FPGA_RET_PARAM_NUM*2*i)
      results(i).qEnd = bufRet(3+FPGA_RET_PARAM_NUM*2*i)
      results(i).rBeg = bufRet(4+FPGA_RET_PARAM_NUM*2*i)
      results(i).rEnd = bufRet(5+FPGA_RET_PARAM_NUM*2*i)
      results(i).score = bufRet(6+FPGA_RET_PARAM_NUM*2*i)
      results(i).trueScore = bufRet(7+FPGA_RET_PARAM_NUM*2*i)
      results(i).width = bufRet(8+FPGA_RET_PARAM_NUM*2*i)
      i = i+1
    }
  }


  //Run DPs on FPGA
  def runOnFPGA(taskNum: Int, //number of tasks
		TOTAL_TASK_NUM: Int,
                tasks: Array[ExtParam], // task array
                results: Array[ExtRet] // result array
                ) {
      //Sizes of Kernel Components
      val DATA_SIZE = TOTAL_TASK_NUM * 256
      val RESULT_SIZE = TOTAL_TASK_NUM * 16

      val buf1Len = commonSize + indivSize*taskNum
      val buf1 = ByteBuffer.allocate(DATA_SIZE).order(ByteOrder.nativeOrder())
      buf1.put(tasks(0).oDel.toByte)
      buf1.put(tasks(0).eDel.toByte)
      buf1.put(tasks(0).oIns.toByte)
      buf1.put(tasks(0).eIns.toByte)
      buf1.put(tasks(0).penClip5.toByte)
      buf1.put(tasks(0).penClip3.toByte)
      buf1.put(tasks(0).w.toByte)
      buf1.put(0.toByte)
      buf1.putInt(taskNum)
      buf1.putInt(0)
      buf1.putInt(0)
      buf1.putInt(0)
      buf1.putInt(0)
      buf1.putInt(0)
      var i = 0
      var leftMaxIns = 0
      var leftMaxDel = 0
      var rightMaxIns = 0
      var rightMaxDel = 0
      var taskPos = buf1Len >> 2
      while (i < taskNum) {
        buf1.putShort(tasks(i).leftQlen.toShort)
        buf1.putShort(tasks(i).leftRlen.toShort)
        buf1.putShort(tasks(i).rightQlen.toShort)
        buf1.putShort(tasks(i).rightRlen.toShort)
        buf1.putInt(taskPos)
        taskPos += ((((tasks(i).leftQlen + tasks(i).leftRlen + tasks(i).rightQlen + tasks(i).rightRlen)+1)/2)+3)/4
        buf1.putShort(tasks(i).regScore.toShort)
        buf1.putShort(tasks(i).qBeg.toShort)
        buf1.putShort(tasks(i).h0.toShort)
        buf1.putShort(tasks(i).idx.toShort)
        leftMaxIns = ((tasks(i).leftQlen * tasks(i).mat.max + tasks(i).penClip5 - tasks(i).oIns).toDouble / tasks(i).eIns + 1).toInt
        leftMaxDel = ((tasks(i).leftQlen * tasks(i).mat.max + tasks(i).penClip5 - tasks(i).oDel).toDouble / tasks(i).eDel + 1).toInt
        rightMaxIns = ((tasks(i).rightQlen * tasks(i).mat.max + tasks(i).penClip3 - tasks(i).oIns).toDouble / tasks(i).eIns + 1).toInt
        rightMaxDel = ((tasks(i).rightQlen * tasks(i).mat.max + tasks(i).penClip3 - tasks(i).oDel).toDouble / tasks(i).eDel + 1).toInt
        buf1.putShort(leftMaxIns.toShort)
        buf1.putShort(leftMaxDel.toShort)
        buf1.putShort(rightMaxIns.toShort)
        buf1.putShort(rightMaxDel.toShort)
	buf1.putInt(tasks(i).idx)

        i = i+1
      }
      i = 0
      var j = 0
      var tmpIntVar = 0
      var counter8 = 0
      while (i < taskNum) {
        if (tasks(i).leftQlen > 0) {
          //assert (tasks(i).leftQs.length == tasks(i).leftQlen)
          j = 0
          while (j < tasks(i).leftQlen) {
              counter8 = counter8 + 1
              tmpIntVar = tmpIntVar << 4 | (tasks(i).leftQs(j).toInt & 0x0F)
              if (counter8 % 8 == 0) buf1.putInt(tmpIntVar)
              j = j + 1
          }
          //tasks(i).leftQs.foreach(ele => {buf1.putInt(ele.toInt)})
        }
        if (tasks(i).rightQlen > 0) {
          //assert (tasks(i).rightQs.length == tasks(i).rightQlen)
          j = 0
          while (j < tasks(i).rightQlen) {
              counter8 = counter8 + 1
              tmpIntVar = tmpIntVar << 4 | (tasks(i).rightQs(j).toInt & 0x0F)
              if (counter8 % 8 == 0) buf1.putInt(tmpIntVar)
              j = j + 1
          }
          //tasks(i).rightQs.foreach(ele => {buf1.putInt(ele.toInt)})
        }
        if (tasks(i).leftRlen > 0) {
          //assert (tasks(i).leftRs.length == tasks(i).leftRlen)
          j = 0
          while (j < tasks(i).leftRlen) {
              counter8 = counter8 + 1
              tmpIntVar = tmpIntVar << 4 | (tasks(i).leftRs(j).toInt & 0x0F)
              if (counter8 % 8 == 0) buf1.putInt(tmpIntVar)
              j = j + 1
          }
          //tasks(i).leftRs.foreach(ele => {buf1.putInt(ele.toInt)})
        }
        if (tasks(i).rightRlen > 0) {
          //assert (tasks(i).rightRs.length == tasks(i).rightRlen)
          j = 0
          while (j < tasks(i).rightRlen) {
              counter8 = counter8 + 1
              tmpIntVar = tmpIntVar << 4 | (tasks(i).rightRs(j).toInt & 0x0F)
              if (counter8 % 8 == 0) buf1.putInt(tmpIntVar)
              j = j + 1
          }
          //tasks(i).rightRs.foreach(ele => {buf1.putInt(ele.toInt)})
        }
        if (counter8 % 8 != 0) {
          while (counter8 % 8 != 0) {
              tmpIntVar = tmpIntVar << 4
              counter8 = counter8 + 1
          }
          buf1.putInt(tmpIntVar)
	}
        i = i + 1
      }

      val conn = new Connector2FPGA("127.0.0.1", 5555);
      conn.buildConnection( 1 );
      conn.send(taskPos*4)
      conn.send(buf1, taskPos*4);

      val bufRet = conn.receive_short(taskNum * retValues)
      i = 0
      while (i < taskNum) {
        if (results(i) == null) results(i) = new ExtRet
        results(i).idx = tasks(i).idx
        results(i).qBeg = bufRet.getShort()
        results(i).qEnd = bufRet.getShort()
        results(i).rBeg = bufRet.getShort()
        results(i).rEnd = bufRet.getShort()
        results(i).score = bufRet.getShort()
        results(i).trueScore = bufRet.getShort()
        results(i).width = bufRet.getShort()
        bufRet.getShort()
        i = i+1
      }

      //writer.println("data received");
      //timer1.report( );

      conn.closeConnection();

  }

  /**
    *  The function that calculates the results before Smith-Waterman
    *
    *  @param opt the MemOptType object
    *  @param pacLen the length of PAC array
    *  @param pac the PAC array
    *  @param queryLen the query length (read length)
    *  @param query the read
    *  @param chain one of the mem chains of the read
    *  This parameter is updated iteratively. The number of iterations is the number of chains of this read.
    */

  def calPreResultsOfSW(opt: MemOptType,
                        pacLen: Long,
                        pac: Array[Byte],
                        queryLen: Int,
                        query: Array[Byte],
                        chain: MemChainType
             	        ): SWPreResultType = {

    var rmax: Array[Long] = new Array[Long](2)   
    var srt: Array[SRTType] = new Array[SRTType](chain.seeds.length) 

    // calculate the maximum possible span of this alignment
    rmax = getMaxSpan(opt, pacLen, queryLen, chain)

    // retrieve the reference sequence
    val ret = bnsGetSeq(pacLen, pac, rmax(0), rmax(1))
    var rseq = ret._1
    val rlen = ret._2
    assert(rlen == rmax(1) - rmax(0))

    // Setup the value of srt array
    var i = 0
    while(i < chain.seeds.length) {
      srt(i) = new SRTType(chain.seedsRefArray(i).len, i)
      i += 1
    }
    
    srt = srt.sortBy(s => (s.len, s.index))
    new SWPreResultType(rmax, srt, rseq, rlen)

  }

  def memChainToAlnBatched(opt: MemOptType,
                           pacLen: Long,
                           pac: Array[Byte],
                           queryLenArray: Array[Int],
                           queryArray: Array[Array[Byte]],
                           numOfReads: Int,
                           preResultsOfSW: Array[Array[SWPreResultType]],
                           chainsFilteredArray: Array[Array[MemChainType]],
                           regArrays: Array[MemAlnRegArrayType],
			   useFPGA: Boolean,
			   threshold: Int
                          ){

    // The coordinate for each read: (chain No., seed No.)
    var coordinates: Array[Array[Int]] = new Array[Array[Int]](numOfReads)
    var i = 0
    while (i < numOfReads) {
    	coordinates(i) = new Array[Int](2)
	    i = i+1
    }

    def initializeCoordinates(coordinates: Array[Array[Int]], chainsFilteredArray: Array[Array[MemChainType]], numOfReads: Int): Boolean = {
      var isFinished = true;
      var i = 0
      while (i < numOfReads) {
        coordinates(i)(0) = 0
        // only if there is at least one chain corresponding to read i, the y coordinate will be assigned
        if (chainsFilteredArray(i) != null) {
          coordinates(i)(1) = chainsFilteredArray(i)(0).seeds.length - 1
          isFinished = false;
        }
        else coordinates(i)(1) = -1
        i = i + 1
      }
      isFinished
    }

    def incrementCoordinates(coordinates: Array[Array[Int]], chainsFilteredArray: Array[Array[MemChainType]], numOfReads: Int, start: Int, end: Int): (Boolean, Int, Int) = {
      var isFinished = true;
      var curStart = start;
      var curEnd = end;
      while (curStart < curEnd && isFinished == true) {
        if (chainsFilteredArray(curStart) == null) curStart += 1;
        else if(coordinates(curStart)(1) < 0) curStart += 1;
        else if(coordinates(curStart)(1) == 0 && coordinates(curStart)(0) == chainsFilteredArray(curStart).length - 1) {
        	curStart += 1;
        }
        else isFinished = false;
      }
      var endFlag = true;
      while (curStart < curEnd-1 && endFlag == true) {
        if (chainsFilteredArray(curEnd-1) == null) curEnd -= 1;
        else if(coordinates(curEnd-1)(1) < 0) curEnd -= 1;
        else if(coordinates(curEnd-1)(1) == 0 && coordinates(curEnd-1)(0) == chainsFilteredArray(curEnd-1).length - 1) curEnd -= 1;
        else endFlag = false;
      }
      var i = curStart;
      while (i < curEnd) {
        if (chainsFilteredArray(i) != null) {
          if (coordinates(i)(1) > 0) {
            coordinates(i)(1) -= 1
            //isFinished = false;
          }
          else if (coordinates(i)(1) == 0) {
            if (coordinates(i)(0) == chainsFilteredArray(i).length - 1) {
              coordinates(i)(1) = -1
            }
            else {
              coordinates(i)(0) += 1
              coordinates(i)(1) = chainsFilteredArray(i)(coordinates(i)(0)).seeds.length - 1
              //isFinished = false;
            }
          }
        }
        i = i + 1
      }
      (isFinished, curStart, curEnd)
    }

    var isFinished = initializeCoordinates(coordinates, chainsFilteredArray, numOfReads)
    var seedArray = new Array[MemSeedType](numOfReads)
    var extensionFlags = new Array[Int](numOfReads)
    var overlapFlags = new Array[Int](numOfReads)
    var newRegs = new Array[MemAlnRegType](numOfReads)
    var regFlags = new Array[Boolean](numOfReads)
    var start = 0;
    var end = numOfReads;
    var fpgaExtTasks = new Array[ExtParam](numOfReads)
    var fpgaExtResults = new Array[ExtRet](numOfReads)
    var taskIdx = 0

    while (!isFinished) {

	taskIdx = 0
	var i = start;
	while (i < end) {
	regFlags(i) = false
	if (coordinates(i)(1) >= 0) {
	  seedArray(i) = chainsFilteredArray(i)(coordinates(i)(0)).seedsRefArray(preResultsOfSW(i)(coordinates(i)(0)).srt(coordinates(i)(1)).index)
	  extensionFlags(i) = testExtension(opt, seedArray(i), regArrays(i))
	  overlapFlags(i) = -1
	  if (extensionFlags(i) < regArrays(i).curLength) overlapFlags(i) = checkOverlapping(coordinates(i)(1)+1, seedArray(i), chainsFilteredArray(i)(coordinates(i)(0)), preResultsOfSW(i)(coordinates(i)(0)).srt)
	  if (extensionFlags(i) < regArrays(i).curLength && overlapFlags(i) == chainsFilteredArray(i)(coordinates(i)(0)).seeds.length) {
	    preResultsOfSW(i)(coordinates(i)(0)).srt(coordinates(i)(1)).index = MARKED
	  }
	  else {
            regFlags(i) = true
            var reg = new MemAlnRegType
            reg.width = opt.w
	    // default values
	    //{
              reg.score = seedArray(i).len * opt.a
              reg.trueScore = seedArray(i).len * opt.a
              reg.qBeg = 0
              reg.rBeg = seedArray(i).rBeg
              reg.qEnd = queryLenArray(i)
              reg.rEnd = seedArray(i).rBeg + seedArray(i).len
              // push the current align reg into the temporary array
              newRegs(i) = reg
            //}
            if (seedArray(i).qBeg > 0 || (seedArray(i).qBeg + seedArray(i).len) != queryLenArray(i)) {
	      var extParam = new ExtParam
              extParam.leftQlen = seedArray(i).qBeg
              var ii = 0
	      if (extParam.leftQlen > 0) {
                extParam.leftQs = new Array[Byte](extParam.leftQlen)
                ii = 0
                while(ii < extParam.leftQlen) {
                  extParam.leftQs(ii) = queryArray(i)(extParam.leftQlen - 1 - ii)
                  ii += 1
                }
	        extParam.leftRlen = (seedArray(i).rBeg - (preResultsOfSW(i)(coordinates(i)(0)).rmax)(0)).toInt
	        extParam.leftRs = new Array[Byte](extParam.leftRlen)
                ii = 0
                while(ii < extParam.leftRlen) {
                  extParam.leftRs(ii) = preResultsOfSW(i)(coordinates(i)(0)).rseq(extParam.leftRlen - 1 - ii)
                  ii += 1
                }
	      }
	      else {
		extParam.leftQs = null
		extParam.leftRlen = 0
		extParam.leftRs = null
	      }

	      var qe = seedArray(i).qBeg + seedArray(i).len
              extParam.rightQlen = queryLenArray(i) - qe
	      if (extParam.rightQlen > 0) {
                extParam.rightQs = new Array[Byte](extParam.rightQlen)
                ii = 0
                while(ii < extParam.rightQlen) {
                  extParam.rightQs(ii) = queryArray(i)(ii + qe)
                  ii += 1
                }
	        var re = seedArray(i).rBeg + seedArray(i).len - preResultsOfSW(i)(coordinates(i)(0)).rmax(0)
	        extParam.rightRlen = (preResultsOfSW(i)(coordinates(i)(0)).rmax(1) - preResultsOfSW(i)(coordinates(i)(0)).rmax(0) - re).toInt
	        extParam.rightRs = new Array[Byte](extParam.rightRlen)
                ii = 0
                while(ii < extParam.rightRlen) {
                  extParam.rightRs(ii) = preResultsOfSW(i)(coordinates(i)(0)).rseq(ii + re.toInt)
                  ii += 1
                }
	      }
	      else {
		extParam.rightQs = null
		extParam.rightRlen = 0
		extParam.rightRs = null
	      }
	      extParam.w = opt.w
	      extParam.mat = opt.mat
	      extParam.oDel = opt.oDel
	      extParam.oIns = opt.oIns
	      extParam.eDel = opt.eDel
	      extParam.eIns = opt.eIns
	      extParam.penClip5 = opt.penClip5
	      extParam.penClip3 = opt.penClip3
	      extParam.zdrop = opt.zdrop
	      extParam.h0 = seedArray(i).len * opt.a
	      extParam.regScore = newRegs(i).score
	      extParam.qBeg = seedArray(i).qBeg
              extParam.idx = i
              fpgaExtTasks(taskIdx) = extParam
              taskIdx = taskIdx + 1
            }
	  }
	}
	i = i+1
      }

      if (useFPGA == true) {
        if (taskIdx >= threshold) {
          //val ret = runOnFPGA(taskIdx, numOfReads, fpgaExtTasks, fpgaExtResults)
          val ret = runOnFPGAJNI(taskIdx, fpgaExtTasks, fpgaExtResults)
	}
	else {
          i = 0;
          while (i < taskIdx) {
              fpgaExtResults(i) = extension(fpgaExtTasks(i))
              i = i+1
          }
	}
      }
      else {
        i = 0;
        while (i < taskIdx) {
            fpgaExtResults(i) = extension(fpgaExtTasks(i))
            i = i+1
        }
      }

      i = 0;
      while (i < taskIdx) {
        var tmpIdx = fpgaExtResults(i).idx
        newRegs(tmpIdx).qBeg = fpgaExtResults(i).qBeg
        newRegs(tmpIdx).rBeg = fpgaExtResults(i).rBeg + seedArray(tmpIdx).rBeg
        newRegs(tmpIdx).qEnd = fpgaExtResults(i).qEnd + seedArray(tmpIdx).qBeg + seedArray(tmpIdx).len
        newRegs(tmpIdx).rEnd = fpgaExtResults(i).rEnd + seedArray(tmpIdx).rBeg + seedArray(tmpIdx).len
        newRegs(tmpIdx).score = fpgaExtResults(i).score
        newRegs(tmpIdx).trueScore = fpgaExtResults(i).trueScore
        newRegs(tmpIdx).width = fpgaExtResults(i).width
        i = i+1;
      }
      i = start;
      while (i < end) {
        if (regFlags(i) == true) {
          newRegs(i).seedCov = computeSeedCoverage(chainsFilteredArray(i)(coordinates(i)(0)), newRegs(i))
          regArrays(i).regs(regArrays(i).curLength) = newRegs(i)
          regArrays(i).curLength += 1
	}
	i = i+1
      }
      val increRes = incrementCoordinates(coordinates, chainsFilteredArray, numOfReads, start, end)
      isFinished = increRes._1
      start = increRes._2
      end = increRes._3
    }
  }

  /**
    *  Calculate the maximum possible span of this alignment
    *  This private function is used by memChainToAln()
    *
    *  @param opt the input MemOptType object
    *  @param qLen the query length (the read length)
    */
  private def calMaxGap(opt: MemOptType, qLen: Int): Int = {
    val lenDel = ((qLen * opt.a - opt.oDel).toDouble / opt.eDel.toDouble + 1.0).toInt
    val lenIns = ((qLen * opt.a - opt.oIns).toDouble / opt.eIns.toDouble + 1.0).toInt
    var len = -1

    if(lenDel > lenIns)
      len = lenDel
    else
      len = lenIns

    if(len <= 1) len = 1

    val tmp = opt.w << 1

    if(len < tmp) len
    else tmp
  }
 	

  /** 
    *  Get the max possible span
    *  This private function is used by memChainToAln()
    *
    *  @param opt the input opt object
    *  @param pacLen the length of PAC array
    *  @param queryLen the length of the query (read)
    *  @param chain the input chain
    */
  private def getMaxSpan(opt: MemOptType, pacLen: Long, queryLen: Int, chain: MemChainType): Array[Long] = {
    var rmax: Array[Long] = new Array[Long](2)
    val doublePacLen = pacLen << 1
    rmax(0) = doublePacLen
    rmax(1) = 0

    val seedMinRBeg = chain.seeds.map(seed => 
      { seed.rBeg - ( seed.qBeg + calMaxGap(opt, seed.qBeg) ) } ).min
    val seedMaxREnd = chain.seeds.map(seed => 
      { seed.rBeg + seed.len + (queryLen - seed.qBeg - seed.len) + calMaxGap(opt, queryLen - seed.qBeg - seed.len) } ).max
   
    if(rmax(0) > seedMinRBeg) rmax(0) = seedMinRBeg
    if(rmax(1) < seedMaxREnd) rmax(1) = seedMaxREnd
      
    if(rmax(0) <= 0) rmax(0) = 0
    if(rmax(1) >= doublePacLen) rmax(1) = doublePacLen

    // crossing the forward-reverse boundary; then choose one side
    if(rmax(0) < pacLen && pacLen < rmax(1)) {
      // this works because all seeds are guaranteed to be on the same strand
      if(chain.seedsRefArray(0).rBeg < pacLen) rmax(1) = pacLen
      else rmax(0) = pacLen
    }

    rmax
  }
   
  /**
    *  Test whether extension has been made before
    *  This private function is used by memChainToAln()
    *
    *  @param opt the input opt object
    *  @param seed the input seed
    *  @param regArray the current align registers
    */
  private def testExtension(opt: MemOptType, seed: MemSeedType, regArray: MemAlnRegArrayType): Int = {
    var rDist: Long = -1 
    var qDist: Int = -1
    var maxGap: Int = -1
    var minDist: Int = -1
    var w: Int = -1
    var breakIdx: Int = regArray.maxLength
    var i = 0
    var isBreak = false

    while(i < regArray.curLength && !isBreak) {
        
      if(seed.rBeg >= regArray.regs(i).rBeg && (seed.rBeg + seed.len) <= regArray.regs(i).rEnd && 
        seed.qBeg >= regArray.regs(i).qBeg && (seed.qBeg + seed.len) <= regArray.regs(i).qEnd) {
        // qDist: distance ahead of the seed on query; rDist: on reference
        qDist = seed.qBeg - regArray.regs(i).qBeg
        rDist = seed.rBeg - regArray.regs(i).rBeg

        if(qDist < rDist) minDist = qDist 
        else minDist = rDist.toInt

        // the maximal gap allowed in regions ahead of the seed
        maxGap = calMaxGap(opt, minDist)

        // bounded by the band width          
        if(maxGap < opt.w) w = maxGap
        else w = opt.w
          
        // the seed is "around" a previous hit
        if((qDist - rDist) < w && (rDist - qDist) < w) { 
          breakIdx = i 
          isBreak = true
        }

        if(!isBreak) {
          // the codes below are similar to the previous four lines, but this time we look at the region behind
          qDist = regArray.regs(i).qEnd - (seed.qBeg + seed.len)
          rDist = regArray.regs(i).rEnd - (seed.rBeg + seed.len)
          
          if(qDist < rDist) minDist = qDist
          else minDist = rDist.toInt

          maxGap = calMaxGap(opt, minDist)

          if(maxGap < opt.w) w = maxGap
          else w = opt.w

          if((qDist - rDist) < w && (rDist - qDist) < w) {
            breakIdx = i
            isBreak = true
          }          
        }
      }

      i += 1
    }

    if(isBreak) breakIdx
    else i
  }
    
  /**
    *  Further check overlapping seeds in the same chain
    *  This private function is used by memChainToAln()
    *
    *  @param startIdx the index return by the previous testExtension() function
    *  @param seed the current seed
    *  @param chain the input chain
    *  @param srt the srt array, which record the length and the original index on the chain
    */ 
  private def checkOverlapping(startIdx: Int, seed: MemSeedType, chain: MemChainType, srt: Array[SRTType]): Int = {
    var breakIdx = chain.seeds.length
    var i = startIdx
    var isBreak = false

    while(i < chain.seeds.length && !isBreak) {
      if(srt(i).index != MARKED) {
        val targetSeed = chain.seedsRefArray(srt(i).index)

        // only check overlapping if t is long enough; TODO: more efficient by early stopping
        // NOTE: the original implementation may be not correct!!!
        if(targetSeed.len >= seed.len * 0.95) {
          if(seed.qBeg <= targetSeed.qBeg && (seed.qBeg + seed.len - targetSeed.qBeg) >= (seed.len>>2) && (targetSeed.qBeg - seed.qBeg) != (targetSeed.rBeg - seed.rBeg)) {
            breakIdx = i
            isBreak = true
          }
            
          if(!isBreak && targetSeed.qBeg <= seed.qBeg && (targetSeed.qBeg + targetSeed.len - seed.qBeg) >= (seed.len>>2) && (seed.qBeg - targetSeed.qBeg) != (seed.rBeg - targetSeed.rBeg)) {
            breakIdx = i
            isBreak = true
          }
        }
      }

      i += 1
    }

    if(isBreak) breakIdx
    else i
  }

  private def extension(extParam: ExtParam): ExtRet = {
    var aw0 = extParam.w
    var aw1 = extParam.w
    var qle = -1
    var tle = -1
    var gtle = -1
    var gscore = -1
    var maxoff = -1
    var i = 0
    var isBreak = false
    var prev: Int = -1
    var regScore: Int = extParam.regScore

    var extRet = new ExtRet
    extRet.qBeg = 0
    extRet.rBeg = 0
    extRet.qEnd = extParam.rightQlen
    extRet.rEnd = 0
    extRet.trueScore = extParam.regScore

    if (extParam.leftQlen > 0) {
      while(i < MAX_BAND_TRY && !isBreak) {
        prev = regScore
        aw0 = extParam.w << i
        //val results = SWExtend(seed.qBeg, qs, tmp, rs, 5, opt.mat, opt.oDel, opt.eDel, opt.oIns, opt.eIns, aw, opt.penClip5, opt.zdrop, seed.len * opt.a)
        val results = SWExtend(extParam.leftQlen, extParam.leftQs, extParam.leftRlen, extParam.leftRs, 5, extParam.mat, extParam.oDel, extParam.eDel, extParam.oIns, extParam.eIns, aw0, extParam.penClip5, extParam.zdrop, extParam.h0)
        regScore = results(0)
        qle = results(1)
        tle = results(2)
        gtle = results(3)
        gscore = results(4)
        maxoff = results(5)
        if(regScore == prev || ( maxoff < (aw0 >> 1) + (aw0 >> 2) ) ) isBreak = true

        i += 1
      }
      extRet.score = regScore

      // check whether we prefer to reach the end of the query
      // local extension
      if(gscore <= 0 || gscore <= (regScore - extParam.penClip5)) {
        extRet.qBeg = extParam.qBeg - qle
        extRet.rBeg = -tle
        extRet.trueScore = regScore
        //print("LE, qle " + qle + ", tle " + tle)   // testing
      }
      // to-end extension
      else {
        extRet.qBeg = 0
        extRet.rBeg = -gtle
        extRet.trueScore = gscore
        //print("TEE, gtle " + gtle)   // testing
      }
    }

    if (extParam.rightQlen > 0) {
      i = 0
      isBreak = false
      var sc0 = regScore
      while(i < MAX_BAND_TRY && !isBreak) {
        prev = regScore
        aw1 = extParam.w << i
        val results = SWExtend(extParam.rightQlen, extParam.rightQs, extParam.rightRlen, extParam.rightRs, 5, extParam.mat, extParam.oDel, extParam.eDel, extParam.oIns, extParam.eIns, aw1, extParam.penClip3, extParam.zdrop, sc0)
        regScore = results(0)
        qle = results(1)
        tle = results(2)
        gtle = results(3)
        gscore = results(4)
        maxoff = results(5)
        if(regScore == prev || ( maxoff < (aw1 >> 1) + (aw1 >> 2) ) ) isBreak = true

        i += 1
      }
      extRet.score = regScore

      // check whether we prefer to reach the end of the query
      // local extension
      if(gscore <= 0 || gscore <= (regScore - extParam.penClip3)) {
        extRet.qEnd = qle
        extRet.rEnd = tle
        extRet.trueScore += regScore - sc0
      }
      else {
        extRet.qEnd = extParam.rightQlen 
        extRet.rEnd = gtle
        extRet.trueScore += gscore - sc0
      }
    }
    if (aw0 > aw1) extRet.width = aw0
    else extRet.width = aw1
    extRet.idx = extParam.idx

    //println(", qb " + regResult.qBeg + ", rb " + regResult.rBeg + ", truesc " + regResult.trueScore)   // testing
    extRet
  }
    
  /** 
    *  Compute the seed coverage
    *  This private function is used by memChainToAln()
    * 
    *  @param chain the input chain
    *  @param reg the current align register after left/right extension is done 
    */
  private def computeSeedCoverage(chain: MemChainType, reg: MemAlnRegType): Int = {
    var seedcov = 0
    var i = 0
    
    while(i < chain.seeds.length) {
      // seed fully contained
      if(chain.seedsRefArray(i).qBeg >= reg.qBeg && 
         chain.seedsRefArray(i).qBeg + chain.seedsRefArray(i).len <= reg.qEnd &&
         chain.seedsRefArray(i).rBeg >= reg.rBeg &&
         chain.seedsRefArray(i).rBeg + chain.seedsRefArray(i).len <= reg.rEnd)
        seedcov += chain.seedsRefArray(i).len   // this is not very accurate, but for approx. mapQ, this is good enough

      i += 1
    }

    seedcov
  }


  /**
    *  Read class (testing use)
    */
  class ReadChain(chains_i: MutableList[MemChainType], seq_i: Array[Byte]) {
    var chains: MutableList[MemChainType] = chains_i
    var seq: Array[Byte] = seq_i
  }

  /**
    *  Member variable of all reads (testing use)
    */ 
  var testReadChains: MutableList[ReadChain] = new MutableList
  
  /**
    *  Read the test chain data generated from bwa-0.7.8 (C version) (testing use)
    *
    *  @param fileName the test data file name
    */
  def readTestData(fileName: String) {
    val reader = new BufferedReader(new FileReader(fileName))

    var line = reader.readLine
    var chains: MutableList[MemChainType] = new MutableList
    var chainPos: Long = 0
    var seeds: MutableList[MemSeedType] = new MutableList
    var seq: Array[Byte] = new Array[Byte](101)  // assume the size to be 101 (not true for all kinds of reads)

    while(line != null) {
      val lineFields = line.split(" ")      

      // Find a sequence
      if(lineFields(0) == "Sequence") {
        chains = new MutableList
        seq = lineFields(2).getBytes
        seq = seq.map(s => (s - 48).toByte) // ASCII => Byte(Int)
      }
      // Find a chain
      else if(lineFields(0) == "Chain") {
        seeds = new MutableList
        chainPos = lineFields(1).toLong
      }
      // Fina a seed
      else if(lineFields(0) == "Seed") {
        seeds += (new MemSeedType(lineFields(1).toLong, lineFields(2).toInt, lineFields(3).toInt))
      }
      // append the current list
      else if(lineFields(0) == "ChainEnd") {
        val cur_seeds = seeds
        chains += (new MemChainType(chainPos, cur_seeds))
      }
      // append the current list
      else if(lineFields(0) == "SequenceEnd") {
        val cur_chains = chains
        val cur_seq = seq 
        testReadChains += (new ReadChain(cur_chains, seq))
      }

      line = reader.readLine
    }

  }


  /**
    *  Print all the chains (and seeds) from all input reads 
    *  (Only for testing use)
    */
  def printAllReads() {
    def printChains(chains: MutableList[MemChainType]) {
      println("Sequence");
      def printSeeds(seeds: MutableList[MemSeedType]) {
        seeds.foreach(s => println("Seed " + s.rBeg + " " + s.qBeg + " " + s.len))
      }
    
      chains.map(p => {
        println("Chain " + p.pos + " " + p.seeds.length)
        printSeeds(p.seeds)
                      } )
    }

    testReadChains.foreach(r => printChains(r.chains))
  }

}

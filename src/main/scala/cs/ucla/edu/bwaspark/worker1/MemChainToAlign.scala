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


package cs.ucla.edu.bwaspark.worker1

import scala.collection.mutable.MutableList

import cs.ucla.edu.bwaspark.datatype._
import cs.ucla.edu.bwaspark.util.BNTSeqUtil._
import cs.ucla.edu.bwaspark.util.SWUtil._

// Used for read test input data
import java.io.{FileReader, BufferedReader}

object MemChainToAlign {
  val MAX_BAND_TRY = 2    
  val MARKED = -2

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


  /**
    *  The main function of memChainToAlign class
    *
    *  @param opt the MemOptType object
    *  @param pacLen the length of PAC array
    *  @param pac the PAC array
    *  @param queryLen the query length (read length)
    *  @param chain one of the mem chains of the read
    *  @param regArray the input alignment registers, which are the registers from the output of the previous call on MemChainToAln().
    *              This parameter is updated iteratively. The number of iterations is the number of chains of this read.
    */
  def memChainToAln(opt: MemOptType, pacLen: Long, pac: Array[Byte], queryLen: Int, query: Array[Byte], 
    chain: MemChainType, regArray: MemAlnRegArrayType) {

    var rmax: Array[Long] = new Array[Long](2)   
    var srt: Array[SRTType] = new Array[SRTType](chain.seeds.length) 
    var aw: Array[Int] = new Array[Int](2)

 
    // calculate the maximum possible span of this alignment
    rmax = getMaxSpan(opt, pacLen, queryLen, chain)
    //println("rmax(0): " + rmax(0) + ", rmax(1): " + rmax(1))  // debugging message

    // retrieve the reference sequence
    val ret = bnsGetSeq(pacLen, pac, rmax(0), rmax(1))
    var rseq = ret._1
    val rlen = ret._2
    assert(rlen == rmax(1) - rmax(0))

    // debugging message
    //println(rlen)     
    //for(i <- 0 until rlen.toInt)
      //print(rseq(i).toInt)
    //println

    // Setup the value of srt array
    var i = 0
    while(i < chain.seeds.length) {
      srt(i) = new SRTType(chain.seedsRefArray(i).len, i)
      i += 1
    }

    srt = srt.sortBy(s => (s.len, s.index))
    //srt.map(s => println("(" + s.len + ", " + s.index + ")") )  // debugging message

    // The main for loop    
    var k = chain.seeds.length - 1
    while(k >= 0) {
      val seed = chain.seedsRefArray( srt(k).index )
      var i = testExtension(opt, seed, regArray)
     
      var checkoverlappingRet = -1

      if(i < regArray.curLength) checkoverlappingRet = checkOverlapping(k + 1, seed, chain, srt)
      
      // no overlapping seeds; then skip extension
      if((i < regArray.curLength) && (checkoverlappingRet == chain.seeds.length)) {
        srt(k).index = MARKED  // mark that seed extension has not been performed
      }
      else {
        // push the current align reg into the output list
        // initialize a new alnreg
        var reg = new MemAlnRegType
        reg.width = opt.w
        aw(0) = opt.w
        aw(1) = opt.w
        reg.score = -1
        reg.trueScore = -1
     
        // left extension
        if(seed.qBeg > 0) {
          val ret = leftExtension(opt, seed, rmax, query, rseq, reg) 
          reg = ret._1
          aw(0) = ret._2
        }
        else {
          reg.score = seed.len * opt.a
          reg.trueScore = seed.len * opt.a
          reg.qBeg = 0
          reg.rBeg = seed.rBeg
        }
            
        // right extension
        if((seed.qBeg + seed.len) != queryLen) {
          val ret = rightExtension(opt, seed, rmax, query, queryLen, rseq, reg)
          reg = ret._1
          aw(1) = ret._2
        }
        else {
          reg.qEnd = queryLen
          reg.rEnd = seed.rBeg + seed.len
        }
  
        reg.seedCov = computeSeedCoverage(chain, reg)

        if(aw(0) > aw(1)) reg.width = aw(0)
        else reg.width = aw(1)

        // push the current align reg into the output array
        regArray.regs(regArray.curLength) = reg
        regArray.curLength += 1
      }

      k -= 1
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

  /**
    *  Left extension of the current seed
    *  This private function is used by memChainToAln()
    *
    *  @param opt the input MemOptType object
    *  @param seed the current seed
    *  @param rmax the calculated maximal range
    *  @param query the query (read)
    *  @param rseq the reference sequence
    *  @param reg the current align register before doing left extension (the value is not complete yet)
    */
  private def leftExtension(opt: MemOptType, seed: MemSeedType, rmax: Array[Long], query: Array[Byte], rseq: Array[Byte], reg: MemAlnRegType): (MemAlnRegType, Int) = {
    var aw = 0
    val tmp = (seed.rBeg - rmax(0)).toInt
    var qs = new Array[Byte](seed.qBeg)
    var rs = new Array[Byte](tmp)
    var qle = -1
    var tle = -1
    var gtle = -1
    var gscore = -1
    var maxoff = -1

    var regResult = reg
    
    var i = 0
    while(i < seed.qBeg) {
      qs(i) = query(seed.qBeg - 1 - i)
      i += 1
    }

    i = 0
    while(i < tmp) {
      rs(i) = rseq(tmp - 1 - i)
      i += 1
    }
    
    i = 0
    var isBreak = false
    while(i < MAX_BAND_TRY && !isBreak) {
      var prev = regResult.score
      aw = opt.w << i
      val results = SWExtend(seed.qBeg, qs, tmp, rs, 5, opt.mat, opt.oDel, opt.eDel, opt.oIns, opt.eIns, aw, opt.penClip5, opt.zdrop, seed.len * opt.a)
      regResult.score = results(0)
      qle = results(1)
      tle = results(2)
      gtle = results(3)
      gscore = results(4)
      maxoff = results(5)

      if(regResult.score == prev || ( maxoff < (aw >> 1) + (aw >> 2) ) ) isBreak = true

      i += 1
    }

    // check whether we prefer to reach the end of the query
    // local extension
    if(gscore <= 0 || gscore <= (regResult.score - opt.penClip5)) {
      regResult.qBeg = seed.qBeg - qle
      regResult.rBeg = seed.rBeg - tle
      regResult.trueScore = regResult.score
    }
    // to-end extension
    else {
      regResult.qBeg = 0
      regResult.rBeg = seed.rBeg - gtle
      regResult.trueScore = gscore
    }

    (regResult, aw)
  }

  /**
    *  Right extension of the current seed
    *  This private function is used by memChainToAln()
    *
    *  @param opt the input MemOptType object
    *  @param seed the current seed
    *  @param rmax the calculated maximal range
    *  @param query the query (read)
    *  @param queryLen the length of this query
    *  @param rseq the reference sequence
    *  @param reg the current align register before doing left extension (the value is not complete yet)
    */
  private def rightExtension(opt: MemOptType, seed: MemSeedType, rmax: Array[Long], query: Array[Byte], queryLen: Int, rseq: Array[Byte], reg: MemAlnRegType): (MemAlnRegType, Int) = {
    var aw = 0
    var regResult = reg
    var qe = seed.qBeg + seed.len
    var re = seed.rBeg + seed.len - rmax(0)
    var sc0 = regResult.score
    var qle = -1
    var tle = -1
    var gtle = -1
    var gscore = -1
    var maxoff = -1

    assert(re >= 0)

    var qeArray = new Array[Byte](queryLen - qe)
    var i = 0
    // fill qeArray
    while(i < (queryLen - qe)) {
      qeArray(i) = query(qe + i)
      i += 1
    }

    var reArray = new Array[Byte]((rmax(1) - rmax(0) - re).toInt)
    // fill reArray
    i = 0
    while(i < (rmax(1) - rmax(0) - re).toInt) {
      reArray(i) = rseq(re.toInt + i)
      i += 1
    }

    i = 0
    var isBreak = false
    while(i < MAX_BAND_TRY && !isBreak) {
      var prev = regResult.score
      aw = opt.w << i
      val results = SWExtend(queryLen - qe, qeArray, (rmax(1) - rmax(0) - re).toInt, reArray, 5, opt.mat, opt.oDel, opt.eDel, opt.oIns, opt.eIns, aw, opt.penClip3, opt.zdrop, sc0)
      regResult.score = results(0)
      qle = results(1)
      tle = results(2)
      gtle = results(3)
      gscore = results(4)
      maxoff = results(5)

      if(regResult.score == prev || ( maxoff < (aw >> 1) + (aw >> 2) ) ) isBreak = true

      i += 1
    }

    // check whether we prefer to reach the end of the query
    // local extension
    if(gscore <= 0 || gscore <= (regResult.score - opt.penClip3)) {
      regResult.qEnd = qe + qle
      regResult.rEnd = rmax(0) + re + tle
      regResult.trueScore += regResult.score - sc0
    }
    else {
      regResult.qEnd = queryLen
      regResult.rEnd = rmax(0) + re + gtle
      regResult.trueScore += gscore - sc0
    }

    (regResult, aw)
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

}

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


import cs.ucla.edu.bwaspark.datatype._
import cs.ucla.edu.bwaspark.worker1.BWAMemWorker1._
import scala.collection.mutable.MutableList
import java.util.TreeSet
import java.util.Comparator
import cs.ucla.edu.bwaspark.worker1.MemChain._
import cs.ucla.edu.bwaspark.worker1.MemChainFilter._
import cs.ucla.edu.bwaspark.worker1.MemChainToAlign._
import cs.ucla.edu.bwaspark.worker1.MemSortAndDedup._
import cs.ucla.edu.bwaspark.debug.DebugFlag._

import java.io._

object TestWorker1 {

  def main(args: Array[String]) {

    //loading index files
    val bwaIdx = new BWAIdxType
    val prefix = "/home/pengwei/genomics/ReferenceMetadata/human_g1k_v37.fasta"
    bwaIdx.load(prefix, 0)

    //loading BWA MEM options
    val bwaMemOpt = new MemOptType
    bwaMemOpt.load()

    //loading reads
//    val rawData = "TTACTCGTGATGTGTGTCCTCAACTAAAGGAGTAGAACTTTTCTTTTCATAGAGAAGTTTTGAAACGCTCTTTTTGTGGAATCTGCAAGTGGATATTTGGC" //read 0
//    val rawData = "ATAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCGTAACCCTAACCGTAACCCTCACCCTAACCATAAC" //read 1
    val rawData = "TTAGGGTTAGGGTTAGGGTTAGGGTTAGGGTTAGGTTAAGGGTAAGGGTTAGGGTTAGGGTTAGGTTTGGGTTAGGGTTAGGGTTAGGGTTAGGGTTAGGG"
    //val goldenRef = "33013123203232323113100130002202302001333313333103020200233332000121313333323220031321002322030333221"
    val seq: Array[Int] = rawData.map(ele => ele.toInt).toArray 

    debugLevel = 1

    //test bwaMemWork1
//    val aligns = bwaMemWorker1(bwaMemOpt, bwaIdx.bwt, bwaIdx.bns, bwaIdx.pac, null, seq.length, seq)

    //test the first step: memChain
      def locusEncode(locus: Int): Byte = {
        //transforming from A/C/G/T to 0,1,2,3
        locus match {
          case 'A' => 0
          case 'a' => 0
          case 'C' => 1
          case 'c' => 1
          case 'G' => 2
          case 'g' => 2
          case 'T' => 3
          case 't' => 3
          case '-' => 5
          case _ => 4
        }
      }
    val read: Array[Byte] = seq.map(ele => locusEncode(ele))

    read.map (ele => print(ele))
    println()


    val chains = generateChains(bwaMemOpt, bwaIdx.bwt, bwaIdx.bns.l_pac, read.length, read)

    def readChainsFromFile(filename: String): Array[MemChainType] = {

      val chainsReader = new BufferedReader(new FileReader(filename)) 
      chainsReader.readLine() //1st line, useless 
      val line = chainsReader.readLine() //2nd line, #chains
      val chains = new Array[MemChainType](line.toInt)
      for (i <- 0 until line.toInt) {
        var aChain = chainsReader.readLine.split(" ")
        var seeds = new MutableList[MemSeedType]
        for (j <- 0 until aChain(1).toInt) {
          val aSeed = chainsReader.readLine.split(" ")
          seeds += new MemSeedType(aSeed(0).toLong, aSeed(1).toInt, aSeed(2).toInt) 
        }
        chains(i) = new MemChainType(aChain(0).toLong, seeds)
      }
      chainsReader.close();
      chains
    }

//    val chainsFromFile = readChainsFromFile("/home/pengwei/genomics/OutputFiles/chains.log")

//    val chainsFiltered = memChainFilter(bwaMemOpt, chainsFromFile)
    val chainsFiltered = memChainFilter(bwaMemOpt, chains)

  }
}

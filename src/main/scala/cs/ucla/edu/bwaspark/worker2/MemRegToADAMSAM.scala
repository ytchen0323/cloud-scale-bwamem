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


package cs.ucla.edu.bwaspark.worker2

import scala.collection.mutable.MutableList
import scala.collection.immutable.Vector
import scala.math.log
import scala.math.abs

import cs.ucla.edu.bwaspark.datatype._
import cs.ucla.edu.bwaspark.sam.SAMHeader
import cs.ucla.edu.bwaspark.util.BNTSeqUtil._
import cs.ucla.edu.bwaspark.util.SWUtil._
import cs.ucla.edu.avro.fastq._

import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.formats.avro.Contig
import org.bdgenomics.adam.models.{SequenceRecord, SequenceDictionary, RecordGroup}

import htsjdk.samtools.SAMRecord

// for test use
import java.io.FileReader
import java.io.BufferedReader
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{Charset, CharsetEncoder, CharacterCodingException}

object MemRegToADAMSAM {
  private val MEM_F_ALL = 0x8
  private val MEM_F_NO_MULTI = 0x10
  private val MEM_MAPQ_COEF = 30.0
  private val int2op = Array('M', 'I', 'D', 'S', 'H')
  private val int2forward = Array('A', 'C', 'G', 'T', 'N')
  private val int2reverse = Array('T', 'G', 'C', 'A', 'N')
 

  /**
    *  Transform the alignment registers to SAM format
    *  
    *  @param opt the input MemOptType object
    *  @param bns the input BNTSeqType object
    *  @param pac the PAC array
    *  @param seq the read
    *  @param seqTrans the read in the Byte format
    *  @param regs the alignment registers to be transformed
    *  @param extraFlag
    *  @param alnIn currently we skip this parameter
    *  @param samHeader the SAM header required to output SAM strings
    *  @return SAM format String
    */
  def memRegToSAMSe(opt: MemOptType, bns: BNTSeqType, pac: Array[Byte], seq: FASTQRecord, seqTrans: Array[Byte], regs: Array[MemAlnRegType], extraFlag: Int, alnMate: MemAlnType, samHeader: SAMHeader): String = {
    var alns: MutableList[MemAlnType] = new MutableList[MemAlnType]

    if(regs != null) {
      var i = 0
      while(i < regs.length) {
        if(regs(i).score >= opt.T) {
          if(regs(i).secondary < 0 || ((opt.flag & MEM_F_ALL) > 0)) {
            if(regs(i).secondary < 0 || regs(i).score >= regs(regs(i).secondary).score * 0.5) {
              var aln = memRegToAln(opt, bns, pac, seq.seqLength, seqTrans, regs(i)) 
              alns += aln
              aln.flag |= extraFlag   // flag secondary
              if(regs(i).secondary >= 0) aln.sub = -1   // don't output sub-optimal score
              if(i > 0 && regs(i).secondary < 0)   // if supplementary
                if((opt.flag & MEM_F_NO_MULTI) > 0) aln.flag |= 0x10000
                else aln.flag |= 0x800

              if(i > 0 && aln.mapq > alns.head.mapq) aln.mapq = alns.head.mapq            
            }
          }
        }

        i += 1
      }
    }

    // no alignments good enough; then write an unaligned record

    var samStr = new SAMString

    if(alns.length == 0) { 
      var aln = memRegToAln(opt, bns, pac, seq.seqLength, seqTrans, null)
      aln.flag |= extraFlag
      var alnList = new Array[MemAlnType](1)
      alnList(0) = aln

      memAlnToSAM(bns, seq, seqTrans, alnList, 0, alnMate, samHeader, samStr)
    }
    else {
      var k = 0
      val alnsArray = alns.toArray
      while(k < alns.size) {
        memAlnToSAM(bns, seq, seqTrans, alnsArray, k, alnMate, samHeader, samStr)
        k += 1
      }
    }

    samStr.str.dropRight(samStr.size - samStr.idx).mkString
  }


  /**
    *  basic hit->SAM conversion
    *  @param l1
    *  @param l2
    *  @param score
    *  @param a
    *  @param q
    *  @param r
    */
   private def inferBw( l1: Int, l2: Int, score: Int, a: Int, q: Int, r: Int) : Int = {
     var w: Int = 0;
     if(l1 == l2 && (((l1 * a) - score) < ((q + r - a) << 1))) {
     }
     else {
       var wDouble: Double = 0
       wDouble = (((if( l1 < l2 ) l1 else l2) * a - score - q).toDouble / r.toDouble + 2.toDouble)
       var absDifference: Int = if( l1 < l2 ) ( l2 - l1) else ( l1 - l2)
       if( wDouble < absDifference ) w = absDifference
       else w = wDouble.toInt
       //w is the returned Integer
     }
     w
   }

  /**
    *  Get the length of the reference segment
    *  @param cigar the input cigar
    */
  private def getRlen(cigar: CigarType) : Int = {
    var l: Int = 0

    if(cigar != null) {
      var k = 0
      while(k < cigar.cigarSegs.size) {
        if(cigar.cigarSegs(k).op == 0 || cigar.cigarSegs(k).op == 2) l += cigar.cigarSegs(k).len

        k += 1
      }
    }
    
    l
  }
	

  /**
    *  Transform the alignment registers to alignment type
    *
    *  @param opt the input MemOptType object
    *  @param bns the input BNTSeqType object
    *  @param pac the PAC array
    *  @param seqLen the length of the input sequence
    *  @param seq the input sequence (NOTE: currently we use Array[Byte] first. may need to be changed back to Array[Char]!!!)
    *  @param reg the input alignment register
    */
  def memRegToAln(opt: MemOptType, bns: BNTSeqType, pac: Array[Byte], seqLen: Int, seq: Array[Byte], reg: MemAlnRegType): MemAlnType = {
    val aln = new MemAlnType

    if(reg == null || reg.rBeg < 0 || reg.rEnd < 0) {
      aln.rid = -1
      aln.pos = -1
      aln.flag |= 0x4
      aln
    }
    else {
      var qb = reg.qBeg
      var qe = reg.qEnd
      var rb = reg.rBeg
      var re = reg.rEnd

      if(reg.secondary < 0) 
        aln.mapq = memApproxMapqSe(opt, reg).toShort
      else
        aln.mapq = 0

      // secondary alignment
      if(reg.secondary >= 0) aln.flag |= 0x100 

      val ret = bwaFixXref2(opt.mat, opt.oDel, opt.eDel, opt.oIns, opt.eIns, opt.w, bns, pac, seq, qb, qe, rb, re)
      qb = ret._1
      qe = ret._2
      rb = ret._3
      re = ret._4
      val iden = ret._5
      if(iden < 0) {
        println("[Error] If you see this message, please let the developer know. Abort. Sorry.")
        assert(false, "bwaFixXref2() problem")
      }

      var tmp = inferBw(qe - qb, (re - rb).toInt, reg.trueScore, opt.a, opt.oDel, opt.eDel)
      var w2 = inferBw(qe - qb, (re - rb).toInt, reg.trueScore, opt.a, opt.oIns, opt.eIns)
      if(w2 < tmp) w2 = tmp
      if(w2 > opt.w) {
        if(w2 > reg.width) w2 = reg.width
      }
      //else w2 = opt.w  // TODO: check if we need this line on long reads. On 1-800bp reads, it does not matter and it should be. (In original C implementation)

      var i = 0
      aln.cigar = null
      var score = 0
      var lastScore = -(1 << 30)

      var isBreak = false
      do {
        // make a copy to pass into bwaGenCigar2
        // if there is performance issue later, we may modify the underlying implementation
        var query: Array[Byte] = new Array[Byte](qe - qb)

        var j = 0
        while(j < (qe - qb)) {
          query(j) = seq(qb + j)
          j += 1
        }
 
        var ret = bwaGenCigar2(opt.mat, opt.oDel, opt.eDel, opt.oIns, opt.eIns, w2, bns.l_pac, pac, qe - qb, query, rb, re)
        score = ret._1
        aln.nCigar = ret._2
        aln.NM = ret._3
        aln.cigar = ret._4

        if(score == lastScore) isBreak = true
    
        if(!isBreak) {
          lastScore = score
          w2 <<= 1
        }

        i += 1
      } while(i < 3 && score < reg.trueScore - opt.a && !isBreak)


      var pos: Long = 0
      var isRev: Int = 0
 
      if(rb < bns.l_pac) {
        val ret = bnsDepos(bns, rb)
        pos = ret._1
        isRev = ret._2
      }
      else {
        val ret = bnsDepos(bns, re - 1)
        pos = ret._1
        isRev = ret._2
      }

      aln.isRev = isRev.toByte

      // squeeze out leading or trailing deletions
      if(aln.nCigar > 0) {
        if(aln.cigar.cigarSegs(0).op == 2) {
          pos += aln.cigar.cigarSegs(0).len
          //aln.cigar.cigarSegs = aln.cigar.cigarSegs.drop(1)
          aln.cigar.cigarSegs = aln.cigar.cigarSegs.tail
          aln.nCigar -= 1
        }
        else if(aln.cigar.cigarSegs(aln.nCigar - 1).op == 2) {            
          aln.cigar.cigarSegs = aln.cigar.cigarSegs.dropRight(1)
          aln.nCigar -= 1
        }
      }

      // add clipping to CIGAR
      if(qb != 0 || qe != seqLen) {
        var clip5 = 0
        var clip3 = 0

        if(isRev > 0) clip5 = seqLen - qe
        else clip5 = qb

        if(isRev > 0) clip3 = qb
        else clip3 = seqLen - qe

        if(clip5 > 0) {
          val cigarSeg = new CigarSegType
          cigarSeg.op = 3
          cigarSeg.len = clip5
          aln.cigar.cigarSegs = aln.cigar.cigarSegs.+:(cigarSeg)  // prepend (Vector type)
          aln.nCigar += 1
        }
        
        if(clip3 > 0) {
          val cigarSeg = new CigarSegType
          cigarSeg.op = 3
          cigarSeg.len = clip3
          aln.cigar.cigarSegs = aln.cigar.cigarSegs :+ cigarSeg  // append (Vector type)
          aln.nCigar += 1
        }
      }

      aln.rid = bnsPosToRid(bns, pos)
      aln.pos = pos - bns.anns(aln.rid).offset
      aln.score = reg.score
      if(reg.sub > reg.csub) aln.sub = reg.sub
      else aln.sub = reg.csub
      aln
    }
  }


  /**
    *  Transform the alignment registers to alignment type
    *
    *  @param bns the input BNTSeqType object
    *  @param seq the input read, store as in the FASTQRecord data structure
    *  @param seqTrans the input read after transformation to the byte array
    *  @param alnList the input alignment list
    *  @param which the id to be processed in the alnList
    *  @param alnMate the mate alignment
    *  @param samHeader the SAM header required to output SAM strings
    *  @param samStr the output SAM string
    */
  def memAlnToSAM(bns: BNTSeqType, seq: FASTQRecord, seqTrans: Array[Byte], alnList: Array[MemAlnType], which: Int, alnMate: MemAlnType, samHeader: SAMHeader, samStr: SAMString) {
    var aln = alnList(which)
    var alnTmp = aln.copy
    var alnMateTmp: MemAlnType = null
    if(alnMate != null) alnMateTmp = alnMate.copy 

    // set flag
    if(alnMateTmp != null) alnTmp.flag |= 0x1 // is paired in sequencing
    if(alnTmp.rid < 0) alnTmp.flag |= 0x4 // is unmapped
    if(alnMateTmp != null && alnMateTmp.rid < 0) alnTmp.flag |= 0x8 // is mate mapped
    if(alnTmp.rid < 0 && alnMateTmp != null && alnMateTmp.rid >= 0) { // copy mate to alignment
      alnTmp.rid = alnMateTmp.rid
      alnTmp.pos = alnMateTmp.pos
      alnTmp.isRev = alnMateTmp.isRev
      alnTmp.nCigar = 0
    }
    if(alnMateTmp != null && alnMateTmp.rid < 0 && alnTmp.rid >= 0) { // copy alignment to mate
      alnMateTmp.rid = alnTmp.rid
      alnMateTmp.pos = alnTmp.pos
      alnMateTmp.isRev = alnTmp.isRev
      alnMateTmp.nCigar = 0
    }
    if(alnTmp.isRev > 0) alnTmp.flag |= 0x10 // is on the reverse strand
    if(alnMateTmp != null && alnMateTmp.isRev > 0) alnTmp.flag |= 0x20 // is mate on the reverse strand
       
    // print up to CIGAR
    // seq.name is a Java ByteBuffer
    // When using decode(seq.name), it is a destructive operation
    // Therefore, we need to create a copy of ByteBuffer for seq.name
    val bb = ByteBuffer.wrap(seq.name.array)
    val name = Charset.forName("ISO-8859-1").decode(bb).array;
    samStr.addCharArray(name)   // QNAME
    samStr.addChar('\t')
    if((alnTmp.flag & 0x10000) > 0) alnTmp.flag = (alnTmp.flag & 0xffff) | 0x100   // FLAG
    else alnTmp.flag = (alnTmp.flag & 0xffff) | 0
    samStr.addCharArray(alnTmp.flag.toString.toCharArray)
    samStr.addChar('\t')
    if(alnTmp.rid >= 0) { // with coordinate
      samStr.addCharArray(bns.anns(alnTmp.rid).name.toCharArray)   // RNAME
      samStr.addChar('\t')
      samStr.addCharArray((alnTmp.pos + 1).toString.toCharArray)   // POS
      samStr.addChar('\t')
      samStr.addCharArray(alnTmp.mapq.toString.toCharArray)   // MAPQ
      samStr.addChar('\t')

      if(alnTmp.nCigar > 0) {   // aligned
        var i = 0
        while(i < alnTmp.nCigar) {
          var c = alnTmp.cigar.cigarSegs(i).op
          if(c == 3 || c == 4) 
            if(which > 0) c = 4   // use hard clipping for supplementary alignments
            else c = 3
          samStr.addCharArray(alnTmp.cigar.cigarSegs(i).len.toString.toCharArray)
          samStr.addChar(int2op(c))
          i += 1
        }
      }
      else samStr.addChar('*')
    }
    else samStr.addCharArray("*\t0\t0\t*".toCharArray)   // without coordinte
    samStr.addChar('\t')

    // print the mate position if applicable
    if(alnMateTmp != null && alnMateTmp.rid >= 0) {
      if(alnTmp.rid == alnMateTmp.rid) samStr.addChar('=')
      else samStr.addCharArray(bns.anns(alnMateTmp.rid).name.toString.toCharArray)
      samStr.addChar('\t')
      samStr.addCharArray((alnMateTmp.pos + 1).toString.toCharArray)
      samStr.addChar('\t')
      if(alnTmp.rid == alnMateTmp.rid) {
        var p0: Long = -1
        var p1: Long = -1
        if(alnTmp.isRev > 0) p0 = alnTmp.pos + getRlen(alnTmp.cigar) - 1
        else p0 = alnTmp.pos
        if(alnMateTmp.isRev > 0) p1 = alnMateTmp.pos + getRlen(alnMateTmp.cigar) - 1
        else p1 = alnMateTmp.pos
        if(alnMateTmp.nCigar == 0 || alnTmp.nCigar == 0) samStr.addChar('0')
        else {
          if(p0 > p1) samStr.addCharArray((-(p0 - p1 + 1)).toString.toCharArray)
          else if(p0 < p1) samStr.addCharArray((-(p0 - p1 - 1)).toString.toCharArray)
          else samStr.addCharArray((-(p0 - p1)).toString.toCharArray)
        }
      }
      else samStr.addChar('0')
    }
    else samStr.addCharArray("*\t0\t0".toCharArray)
    samStr.addChar('\t')
    
    // print SEQ and QUAL
    if((alnTmp.flag & 0x100) > 0) {   // for secondary alignments, don't write SEQ and QUAL
      samStr.addCharArray("*\t*".toCharArray)
    }
    else if(alnTmp.isRev == 0) {   // the forward strand
      var qb = 0
      var qe = seq.seqLength

      if(alnTmp.nCigar > 0) {
        if(which > 0 && (alnTmp.cigar.cigarSegs(0).op == 4 || alnTmp.cigar.cigarSegs(0).op == 3)) qb += alnTmp.cigar.cigarSegs(0).len
        if(which > 0 && (alnTmp.cigar.cigarSegs(alnTmp.nCigar - 1).op == 4 || alnTmp.cigar.cigarSegs(alnTmp.nCigar - 1).op == 3)) qe -= alnTmp.cigar.cigarSegs(alnTmp.nCigar - 1).len
      }

      var i = qb
      while(i < qe) {
        samStr.addChar(int2forward(seqTrans(i)))
        i += 1
      }
      samStr.addChar('\t')

      // seq.quality is a Java ByteBuffer
      // When using decode(seq.quality), it is a destructive operation
      // Therefore, we need to create a copy of ByteBuffer for seq.quality
      val bb = ByteBuffer.wrap(seq.quality.array)
      val qual = Charset.forName("ISO-8859-1").decode(bb).array;
      if(qual.size > 0) {
        var i = qb
        while(i < qe) {
          samStr.addChar(qual(i))
          i += 1
        }
      }
      else samStr.addChar('*')
    }
    else {   // the reverse strand
      var qb = 0
      var qe = seq.seqLength
      
      if(alnTmp.nCigar > 0) {
        if(which > 0 && (alnTmp.cigar.cigarSegs(0).op == 4 || alnTmp.cigar.cigarSegs(0).op == 3)) qe -= alnTmp.cigar.cigarSegs(0).len
        if(which > 0 && (alnTmp.cigar.cigarSegs(alnTmp.nCigar - 1).op == 4 || alnTmp.cigar.cigarSegs(alnTmp.nCigar - 1).op == 3)) qb += alnTmp.cigar.cigarSegs(alnTmp.nCigar - 1).len
      }

      var i = qe - 1
      while(i >= qb) {
        samStr.addChar(int2reverse(seqTrans(i)))
        i -= 1
      }
      samStr.addChar('\t')

      // seq.quality is a Java ByteBuffer
      // When using decode(seq.quality), it is a destructive operation
      // Therefore, we need to create a copy of ByteBuffer for seq.quality
      val bb = ByteBuffer.wrap(seq.quality.array)
      val qual = Charset.forName("ISO-8859-1").decode(bb).array;
      if(qual.size > 0) {
        var i = qe - 1
        while(i >= qb) {
          samStr.addChar(qual(i))
          i -= 1
        }
      }
      else samStr.addChar('*')
    }

    // print optional tags
    if(alnTmp.nCigar > 0) {
      samStr.addCharArray("\tNM:i:".toCharArray)
      samStr.addCharArray(alnTmp.NM.toString.toCharArray)
      samStr.addCharArray("\tMD:Z:".toCharArray)
      samStr.addCharArray(alnTmp.cigar.cigarStr.toCharArray)
    }
    if(alnTmp.score >= 0) {
      samStr.addCharArray("\tAS:i:".toCharArray)
      samStr.addCharArray(alnTmp.score.toString.toCharArray)
    }
    if(alnTmp.sub >= 0) {
      samStr.addCharArray("\tXS:i:".toCharArray)
      samStr.addCharArray(alnTmp.sub.toString.toCharArray)
    }
    // Read group is read using SAMHeader class 
    if(samHeader.bwaReadGroupID != "") {
      samStr.addCharArray("\tRG:Z:".toCharArray)
      samStr.addCharArray(samHeader.bwaReadGroupID.toCharArray)
    }
    
    if((alnTmp.flag & 0x100) == 0) { // not multi-hit
      var i = 0
      var isBreak = false
      while(i < alnList.size && !isBreak) {
        if(i != which && (alnList(i).flag & 0x100) == 0) { 
          isBreak = true 
          i -= 1
        }
        i += 1
      }

      if(i < alnList.size) { // there are other primary hits; output them
        samStr.addCharArray("\tSA:Z:".toCharArray)
        var j = 0
        while(j < alnList.size) {
          if(j != which && (alnList(j).flag & 0x100) == 0) { // proceed if: 1) different from the current; 2) not shadowed multi hit
            samStr.addCharArray(bns.anns(alnList(j).rid).name.toCharArray)
            samStr.addChar(',')
            samStr.addCharArray((alnList(j).pos + 1).toString.toCharArray)
            samStr.addChar(',')
            if(alnList(j).isRev == 0) samStr.addChar('+')
            else samStr.addChar('-')
            samStr.addChar(',')
            
            var k = 0
            while(k < alnList(j).nCigar) {
              samStr.addCharArray(alnList(j).cigar.cigarSegs(k).len.toString.toCharArray)
              samStr.addChar(int2op(alnList(j).cigar.cigarSegs(k).op))
              k += 1
            }

            samStr.addChar(',')
            samStr.addCharArray(alnList(j).mapq.toString.toCharArray)
            samStr.addChar(',')
            samStr.addCharArray(alnList(j).NM.toString.toCharArray)
            samStr.addChar(';')

          }
          j += 1
        }
      } 
    }

    // seq.comment is a Java ByteBuffer
    // When using decode(seq.comment), it is a destructive operation
    // Therefore, we need to create a copy of ByteBuffer for seq.comment
    // DO NOT include comments in the SAM output
    // It seems that Samtools does not take this
    
    //val bbComment = ByteBuffer.wrap(seq.comment.array)
    //val comment = Charset.forName("ISO-8859-1").decode(bbComment).array;
    //if(comment.size > 0) {
    //  samStr.addChar('\t')
    //  samStr.addCharArray(comment)
    //}

    samStr.addChar('\n')

  }

  /**
    *  Calculate the approximate mapq value
    *
    *  @param opt the input MemOptType object
    *  @param reg the input alignment register
    */
  def memApproxMapqSe(opt: MemOptType, reg: MemAlnRegType): Int = {
    var sub = 0
    
    if(reg.sub > 0) sub = reg.sub
    else sub = opt.minSeedLen * opt.a

    if(reg.csub > sub) sub = reg.csub
   
    if(sub >= reg.score) 0
    else {
      var len = 0
      var mapq = 0
      
      if(reg.qEnd - reg.qBeg > reg.rEnd - reg.rBeg) len = reg.qEnd - reg.qBeg
      else len = (reg.rEnd - reg.rBeg).toInt
      
      val identity = 1.0 - (len * opt.a - reg.score).toDouble / (opt.a + opt.b) / len
      if(reg.score == 0) mapq = 0
      else if(opt.mapQCoefLen > 0) {
        var tmp: Double = 1.0
        if(len > opt.mapQCoefLen) tmp = opt.mapQCoefFac / log(len)
        tmp *= identity * identity
        mapq = (6.02 * (reg.score - sub) / opt.a * tmp * tmp + 0.499).toInt
      }
      else {
        mapq = (MEM_MAPQ_COEF * (1.0 - sub.toDouble / reg.score) * log(reg.seedCov) + .499).toInt
        if(identity < 0.95) mapq = (mapq * identity * identity + .499).toInt
      }
   
      if(reg.subNum > 0) mapq -= (4.343 * log(reg.subNum + 1) + .499).toInt
      if(mapq > 60) mapq = 60
      if(mapq < 0) mapq = 0

      mapq
    }

  }  


  /**
    *  Check whether the reference segment range and query segment range are valid
    *
    *  @param mat the mat (score) array
    *  @param oDel oDel in the input MemOptType object
    *  @param eDel eDel in the input MemOptType object
    *  @param oIns oIns in the input MemOptType object
    *  @param eIns eIns in the input MemOptType object
    *  @param w the input weight
    *  @param bns the input BNTSeqType object
    *  @param pac the PAC array
    *  @param query the query (read)
    *  @param qBeg the beginning position of the query
    *  @param qEnd the ending position of the query
    *  @param rBeg the beginning position of the target (reference)
    *  @param rEnd the ending position of the target (reference)
    */
  private def bwaFixXref2(mat: Array[Byte], oDel: Int, eDel: Int, oIns: Int, eIns: Int, w: Int, bns: BNTSeqType, 
    pac: Array[Byte], query: Array[Byte], qBeg: Int, qEnd: Int, rBeg: Long, rEnd: Long): (Int, Int, Long, Long, Int) = {
    var retArray = new Array[Int](5)

    // cross the for-rev boundary; actually with BWA-MEM, we should never come to here
    if(rBeg < bns.l_pac && rEnd > bns.l_pac) {
      println("[ERROR] In BWA-mem, we should never come to here")
      (-1, -1, -1, -1, -1)  // unable to fix
    }
    else {
      val ret = bnsDepos(bns, (rBeg + rEnd) >> 1)  // coordinate of the middle point on the forward strand
      val fm = ret._1
      val isRev = ret._2
      val ra = bns.anns(bnsPosToRid(bns, fm))  // annotation of chr corresponding to the middle point
      var cb = ra.offset
      if(isRev > 0) cb = (bns.l_pac << 1) - (ra.offset + ra.len)
      var ce = cb + ra.len   // chr end
      var qBegRet = qBeg
      var qEndRet = qEnd
      var rBegRet = rBeg
      var rEndRet = rEnd

      // fix is needed
      if(cb > rBeg || ce < rEnd) {
        if(cb < rBeg) cb = rBeg
        if(ce > rEnd) ce = rEnd

        var queryArr: Array[Byte] = new Array[Byte](qEnd - qBeg)
        // make a copy to pass into bwaGenCigar2 
        // if there is performance issue later, we may modify the underlying implementation
        var i = 0
        while(i < (qEnd - qBeg)) {
          queryArr(i) = query(qBeg + i)
          i += 1
        }

        val ret = bwaGenCigar2(mat, oDel, eDel, oIns, eIns, w, bns.l_pac, pac, qEnd - qBeg, queryArr, rBeg, rEnd)
        val numCigar = ret._2
        val cigar = ret._4

        var x = rBeg
        var y = qBeg

        var isBreak = false
        i = 0
        while(i < numCigar && !isBreak) {
          val op = cigar.cigarSegs(i).op
          val len = cigar.cigarSegs(i).len

          if(op == 0) {
            if(x <= cb && cb < x + len) {
              qBegRet = (y + (cb - x)).toInt
              rBegRet = cb
            }
              
            if(x < ce && ce <= x + len) {
              qEndRet = (y + (ce - x)).toInt
              rEndRet = ce
              isBreak = true
            }
            else {
              x += len
              y += len
            }
          }
          else if(op == 1) {
            y += len
          } 
          else if(op == 2) {
            if(x <= cb && cb < x + len) {
              qBegRet = y
              rBegRet = x + len
            }
              
            if(x < ce && ce <= x + len) {
              qEndRet = y
              rEndRet = x
              isBreak = true
            }
            else x += len
          }
          else {
            println("[Error] Should not be here!!!")
            assert(false, "in bwaFixXref2()")
          }

          i += 1
        }
      }
    
      var iden = 0
      if(qBegRet == qEndRet || rBegRet == rEndRet) iden = -2
      (qBegRet, qEndRet, rBegRet, rEndRet, iden)
    }

  }


  /**
    *  Generate the Cigar string
    *
    *  @param mat the mat (score) array
    *  @param oDel oDel in the input MemOptType object
    *  @param eDel eDel in the input MemOptType object
    *  @param oIns oIns in the input MemOptType object
    *  @param eIns eIns in the input MemOptType object
    *  @param w the input weight
    *  @param pacLen the PAC array length
    *  @param pac the PAC array
    *  @param queryLen the length of the query array
    *  @param query the query (read)
    *  @param rBeg the beginning position of the target (reference)
    *  @param rEnd the ending position of the target (reference)
    */
  private def bwaGenCigar2(mat: Array[Byte], oDel: Int, eDel: Int, oIns: Int, eIns: Int, w: Int, pacLen: Long, pac: Array[Byte], 
    queryLen: Int, query_i: Array[Byte], rBeg: Long, rEnd: Long): (Int, Int, Int, CigarType) = {

    var numCigar = 0
    var NM = -1
    var score = 0
    var cigar = new CigarType

    // reject if negative length or bridging the forward and reverse strand
    if(queryLen <= 0 || rBeg >= rEnd || (rBeg < pacLen && rEnd > pacLen)) (0, 0, 0, null)
    else {
      val ret = bnsGetSeq(pacLen, pac, rBeg, rEnd)
      var rseq = ret._1
      val rlen = ret._2

      // possible if out of range
      if(rEnd - rBeg != rlen) (0, 0, 0, null)
      else {
        var query = query_i

        // then reverse both query and rseq; this is to ensure indels to be placed at the leftmost position
        if(rBeg >= pacLen) {
          var i = 0
          while(i < (queryLen >> 1)) {
            var tmp = query(i)
            query(i) = query(queryLen - 1 - i)
            query(queryLen - 1 - i) = tmp

            i += 1
          }
            
          i = 0
          while(i < (rlen >> 1).toInt) {
            var tmp = rseq(i)
            rseq(i) = rseq((rlen - 1 - i).toInt)
            rseq((rlen - 1 - i).toInt) = tmp

            i += 1
          }
        }        
        // no gap; no need to do DP
        if(queryLen == (rEnd - rBeg) && w == 0) {
          // FIXME: due to an issue in mem_reg2aln(), we never come to this block. This does not affect accuracy, but it hurts performance. (in original C implementation)
          var cigarSeg = new CigarSegType
          cigarSeg.len = queryLen 
          cigarSeg.op = 0
          numCigar = 1
          cigar.cigarSegs = cigar.cigarSegs :+ cigarSeg   // append (Vector type)

          var i = 0
          while(i < queryLen) {
            score += mat(rseq(i) * 5 + query(i))
            i += 1
          }
        }
        else {
          val maxIns = ((((queryLen + 1) >> 1) * mat(0) - oIns).toDouble / eIns + 1.0).toInt
          val maxDel = ((((queryLen + 1) >> 1) * mat(0) - oDel).toDouble / eDel + 1.0).toInt
          var maxGap = maxIns
          if(maxIns < maxDel) maxGap = maxDel

          var width = (maxGap + abs((rlen - queryLen) + 1)) >> 1
          if(width > w) width = w
          val minWidth = abs(rlen - queryLen) + 3
          if(width < minWidth) width = minWidth
          // NW alignment
          val ret = SWGlobal(queryLen, query, rlen.toInt, rseq, 5, mat, oDel, eDel, oIns, eIns, width.toInt, numCigar, cigar)
          score = ret._1
          numCigar = ret._2
        }
       
        // compute NM and MD
        // str.l = str.m = *n_cigar * 4; str.s = (char*)cigar; // append MD to CIGAR   
        var int2base = new Array[Char](5)
        var x = 0
        var y = 0
        var u = 0
        var n_mm = 0
        var nGap = 0

        if(rBeg < pacLen) int2base = Array('A', 'C', 'G', 'T', 'N')
        else int2base = Array('T', 'G', 'C', 'A', 'N')        

        var k = 0
        while(k < numCigar) {
          val op = cigar.cigarSegs(k).op
          val len = cigar.cigarSegs(k).len
        
          // match
          if(op == 0) {
            var i = 0
            while(i < len) {
              if(query(x + i) != rseq(y + i)) {
                cigar.cigarStr += u.toString
                cigar.cigarStr += int2base(rseq(y + i))
                n_mm += 1
                u = 0
              }
              else u += 1

              i += 1
            }

            x += len
            y += len
          }
          // deletion
          else if(op == 2) {
            // don't do the following if D is the first or the last CIGAR
            if(k > 0 && k < numCigar - 1) {
              cigar.cigarStr += u.toString
              cigar.cigarStr += '^'
              
              var i = 0
              while(i < len) {
                cigar.cigarStr += int2base(rseq(y + i))
                i += 1
              }
              
              u = 0
              nGap += len
            }

            y += len
          }
          // insertion
          else if(op == 1) {
            x += len
            nGap += len
          }

          k += 1
        }
        
        cigar.cigarStr += u.toString
        var NM = n_mm + nGap

        // reverse back query 
        // This is done in original C implementation. However, in the current implementation, the query is a copy
        // from the original array. Therefore, we do not need to reverse this copy back
        //if(rBeg >= pacLen) 
          //for(i <- 0 to ((queryLen >> 1) - 1)) {
            //var tmp = query(i)
            //query(i) = query(queryLen - 1 - i)
            //query(queryLen - 1 - i) = tmp
          //}
        
        //println(cigar.cigarStr)

        (score, numCigar, NM, cigar)
      }
    }

  }


  // test the correctness of bwaFixXref2 function
  //def testBwaFixXref2(fileName: String, opt: MemOptType, bns: BNTSeqType, pac: Array[Byte]) {
  private def testBwaFixXref2(fileName: String, opt: MemOptType, bns: BNTSeqType, pac: Array[Byte]) {
    
    val reader = new BufferedReader(new FileReader(fileName))
    var line = reader.readLine
    var i = 0

    while(line != null) {
      val seq = line.toCharArray.map(ele => (ele.toByte - 48).toByte)
      //seq.foreach(print)
      //println
      line = reader.readLine
      val lineFields = line.split(" ")
      val qBeg = lineFields(0).toInt
      val qEnd = lineFields(1).toInt
      val rBeg = lineFields(2).toLong
      val rEnd = lineFields(3).toLong

      val ret = bwaFixXref2(opt.mat, opt.oDel, opt.eDel, opt.oIns, opt.eIns, opt.w, bns, pac, seq, qBeg, qEnd, rBeg, rEnd)
      println(ret._1 + " " + ret._2 + " " + ret._3 + " " + ret._4 + " " + ret._5)
      line = reader.readLine
      i += 1
    }
  }


  /**
    *  Transform the alignment registers to ADAM format
    *  
    *  @param opt the input MemOptType object
    *  @param bns the input BNTSeqType object
    *  @param pac the PAC array
    *  @param seq the read
    *  @param seqTrans the read in the Byte format
    *  @param regs the alignment registers to be transformed
    *  @param extraFlag
    *  @param alnIn currently we skip this parameter
    *  @param samHeader the SAM header required to output SAM strings
    *  @param seqDict the sequences (chromosome) dictionary: used for ADAM format output
    *  @param readGroup the read group: used for ADAM format output
    *  @return an array of ADAM format object
    */
  def memRegToADAMSe(opt: MemOptType, bns: BNTSeqType, pac: Array[Byte], seq: FASTQRecord, seqTrans: Array[Byte], regs: Array[MemAlnRegType], extraFlag: Int, 
                     alnMate: MemAlnType, samHeader: SAMHeader, seqDict: SequenceDictionary, readGroup: RecordGroup): Vector[AlignmentRecord] = {
    var alns: MutableList[MemAlnType] = new MutableList[MemAlnType]
    var adamVec: Vector[AlignmentRecord] = scala.collection.immutable.Vector.empty 

    if(regs != null) {
      var i = 0
      while(i < regs.length) {
        if(regs(i).score >= opt.T) {
          if(regs(i).secondary < 0 || ((opt.flag & MEM_F_ALL) > 0)) {
            if(regs(i).secondary < 0 || regs(i).score >= regs(regs(i).secondary).score * 0.5) {
              var aln = memRegToAln(opt, bns, pac, seq.seqLength, seqTrans, regs(i)) 
              alns += aln
              aln.flag |= extraFlag   // flag secondary
              if(regs(i).secondary >= 0) aln.sub = -1   // don't output sub-optimal score
              if(i > 0 && regs(i).secondary < 0)   // if supplementary
                if((opt.flag & MEM_F_NO_MULTI) > 0) aln.flag |= 0x10000
                else aln.flag |= 0x800

              if(i > 0 && aln.mapq > alns.head.mapq) aln.mapq = alns.head.mapq            
            }
          }
        }

        i += 1
      }
    }

    // no alignments good enough; then write an unaligned record
    if(alns.length == 0) { 
      var aln = memRegToAln(opt, bns, pac, seq.seqLength, seqTrans, null)
      aln.flag |= extraFlag
      var alnList = new Array[MemAlnType](1)
      alnList(0) = aln

      adamVec = adamVec :+ memAlnToADAM(bns, seq, seqTrans, alnList, 0, alnMate, samHeader, seqDict, readGroup)
    }
    else {
      var k = 0
      val alnsArray = alns.toArray
      while(k < alns.size) {
        adamVec = adamVec :+ memAlnToADAM(bns, seq, seqTrans, alnsArray, k, alnMate, samHeader, seqDict, readGroup)
        k += 1
      }
    }

    adamVec
  }


  /**
    *  Transform the alignment registers to alignment type (ADAM format)
    *
    *  @param bns the input BNTSeqType object
    *  @param seq the input read, store as in the FASTQRecord data structure
    *  @param seqTrans the input read after transformation to the byte array
    *  @param alnList the input alignment list
    *  @param which the id to be processed in the alnList
    *  @param alnMate the mate alignment
    *  @param samHeader the SAM header required to output ADAM
    *  @param seqDict the sequences (chromosome) dictionary: used for ADAM format output
    *  @param readGroup the read group: used for ADAM format output
    *  @return the output ADAM object
    */
  def memAlnToADAM(bns: BNTSeqType, seq: FASTQRecord, seqTrans: Array[Byte], alnList: Array[MemAlnType], which: Int, alnMate: MemAlnType, 
                   samHeader: SAMHeader, seqDict: SequenceDictionary, readGroup: RecordGroup): AlignmentRecord = {
    val builder: AlignmentRecord.Builder = AlignmentRecord.newBuilder
    var aln = alnList(which)
    var alnTmp = aln.copy
    var alnMateTmp: MemAlnType = null
    if(alnMate != null) alnMateTmp = alnMate.copy 

    // set flag
    if(alnMateTmp != null) alnTmp.flag |= 0x1 // is paired in sequencing
    if(alnTmp.rid < 0) alnTmp.flag |= 0x4 // is unmapped
    if(alnMateTmp != null && alnMateTmp.rid < 0) alnTmp.flag |= 0x8 // is mate mapped
    if(alnTmp.rid < 0 && alnMateTmp != null && alnMateTmp.rid >= 0) { // copy mate to alignment
      alnTmp.rid = alnMateTmp.rid
      alnTmp.pos = alnMateTmp.pos
      alnTmp.isRev = alnMateTmp.isRev
      alnTmp.nCigar = 0
    }
    if(alnMateTmp != null && alnMateTmp.rid < 0 && alnTmp.rid >= 0) { // copy alignment to mate
      alnMateTmp.rid = alnTmp.rid
      alnMateTmp.pos = alnTmp.pos
      alnMateTmp.isRev = alnTmp.isRev
      alnMateTmp.nCigar = 0
    }
    if(alnTmp.isRev > 0) alnTmp.flag |= 0x10 // is on the reverse strand
    if(alnMateTmp != null && alnMateTmp.isRev > 0) alnTmp.flag |= 0x20 // is mate on the reverse strand
       
    // print up to CIGAR
    // seq.name is a Java ByteBuffer
    // When using decode(seq.name), it is a destructive operation
    // Therefore, we need to create a copy of ByteBuffer for seq.name
    val bb = ByteBuffer.wrap(seq.name.array)
    val name = Charset.forName("ISO-8859-1").decode(bb).toString
    builder.setReadName(name)   // QNAME
    if((alnTmp.flag & 0x10000) > 0) alnTmp.flag = (alnTmp.flag & 0xffff) | 0x100   // FLAG
    else alnTmp.flag = (alnTmp.flag & 0xffff) | 0
    
    // write the flag informantion to ADAM object
    // Need to double check "primary, secondary, and supplementary flags"
    if((alnTmp.flag & 0x1) > 0) builder.setReadPaired(true)                   // Bit: 0x1
    if((alnTmp.flag & 0x2) > 0) builder.setProperPair(true)                   // Bit: 0x2
    // follow the ADAM format requirement
    if((alnTmp.flag & 0x4) == 0) {
      builder.setReadMapped(true)                                             // Bit: 0x4
      if((alnTmp.flag & 0x10) > 0) builder.setReadNegativeStrand(true)        // Bit: 0x10
      if((alnTmp.flag & 0x100) == 0) builder.setPrimaryAlignment(true)        // Bit: 0x100
      else {
        if((alnTmp.flag & 0x800) > 0) { 
          builder.setSupplementaryAlignment(true)                             // Bit: 0x800 
          builder.setSecondaryAlignment(false)                                // Bit: 0x100
        }
        else if((alnTmp.flag & 0x800) == 0) {
          builder.setSupplementaryAlignment(false)                            // Bit: 0x800
          builder.setSecondaryAlignment(true)                                 // Bit: 0x100
        }
      }
    }
    else builder.setReadMapped(false)                                         // Bit: 0x4
    if((alnTmp.flag & 0x8) == 0 && (alnMateTmp != null))
      builder.setMateMapped(true)                                             // Bit: 0x8
    if((alnTmp.flag & 0x20) > 0) builder.setMateNegativeStrand(true)          // Bit: 0x20
    //if((alnTmp.flag & 0x40) > 0) builder.setFirstOfPair(true)                 // Bit: 0x40  // In ADAM bdg-format 0.6.1, this field has been removed (was supported in 0.2.0)
    //if((alnTmp.flag & 0x80) > 0) builder.setSecondOfPair(true)                // Bit: 0x80  // In ADAM bdg-format 0.6.1, this field has been removed (was supported in 0.2.0)
    if((alnTmp.flag & 0x200) > 0) builder.setFailedVendorQualityChecks(true)  // Bit: 0x200
    if((alnTmp.flag & 0x400) > 0) builder.setDuplicateRead(true)              // Bit: 0x400
    
    var cigarStrTmp: String = null
    if(alnTmp.rid >= 0) { // with coordinate
      builder.setContig(SequenceRecord.toADAMContig(seqDict(bns.anns(alnTmp.rid).name.toString).get))   // RNAME
      builder.setStart(alnTmp.pos)   // POS
      builder.setMapq(alnTmp.mapq)   // MAPQ

      if(alnTmp.nCigar > 0) {   // aligned
        var cigarStr = new SAMString
        var len = 0 // calculate end in ADAM format
        var i = 0
        while(i < alnTmp.nCigar) {
          var c = alnTmp.cigar.cigarSegs(i).op
          if(c == 3 || c == 4) 
            if(which > 0) c = 4   // use hard clipping for supplementary alignments
            else c = 3
          if(c == 0 || c == 2)
            len += alnTmp.cigar.cigarSegs(i).len
          cigarStr.addCharArray(alnTmp.cigar.cigarSegs(i).len.toString.toCharArray)
          cigarStr.addChar(int2op(c))
          i += 1
        }

        builder.setEnd(alnTmp.pos + len)  // the end field in the ADAM object
        builder.setCigar(cigarStr.toString)   // CIGAR
        cigarStrTmp = new String(cigarStr.toString)
      }
    }

    // print the basesTrimmedFromStart and basesTrimmedFromEnd fields in the ADAM object
    var cigar: String = new String("")
    if(cigarStrTmp != null) cigar = cigarStrTmp
    val startTrim = if (cigar == "") {
      0
    } else {
      val count = cigar.takeWhile(_.isDigit).toInt
      val operator = cigar.dropWhile(_.isDigit).head

      if (operator == 'H') {
        count
      } else {
        0
      }
    }
    val endTrim = if (cigar.endsWith("H")) {
      // must reverse string as takeWhile is not implemented in reverse direction
      cigar.dropRight(1).reverse.takeWhile(_.isDigit).reverse.toInt
    } else {
      0
    }
    builder.setBasesTrimmedFromStart(startTrim)
    builder.setBasesTrimmedFromEnd(endTrim)

    // print the mate position if applicable
    if(alnMateTmp != null && alnMateTmp.rid >= 0) {
      if(alnTmp.rid == alnMateTmp.rid) builder.setMateContig(SequenceRecord.toADAMContig(seqDict(bns.anns(alnTmp.rid).name.toString).get))   // RNEXT
      else builder.setMateContig(SequenceRecord.toADAMContig(seqDict(bns.anns(alnMateTmp.rid).name.toString).get)) 
      builder.setMateAlignmentStart(alnMateTmp.pos)   // PNEXT

      if(alnMateTmp.nCigar > 0) {   // aligned
        var len = 0 // calculate end in ADAM format
        var i = 0
        while(i < alnMateTmp.nCigar) {
          var c = alnMateTmp.cigar.cigarSegs(i).op
          if(c == 0 || c == 2)
            len += alnMateTmp.cigar.cigarSegs(i).len
          i += 1
        }

        builder.setMateAlignmentEnd(alnMateTmp.pos + len)  // the end field in the ADAM object
      }

      // TLEN; calculate the insert distance
      /*
      if(alnTmp.rid == alnMateTmp.rid) {
        var p0: Long = -1
        var p1: Long = -1
        if(alnTmp.isRev > 0) p0 = alnTmp.pos + getRlen(alnTmp.cigar) - 1
        else p0 = alnTmp.pos
        if(alnMateTmp.isRev > 0) p1 = alnMateTmp.pos + getRlen(alnMateTmp.cigar) - 1
        else p1 = alnMateTmp.pos
        if(alnMateTmp.nCigar == 0 || alnTmp.nCigar == 0) samStr.addChar('0')
        else {
          if(p0 > p1) samStr.addCharArray((-(p0 - p1 + 1)).toString.toCharArray)
          else if(p0 < p1) samStr.addCharArray((-(p0 - p1 - 1)).toString.toCharArray)
          else samStr.addCharArray((-(p0 - p1)).toString.toCharArray)
        }
      }
      else samStr.addChar('0')
      */
    }
    
    // print SEQ and QUAL
    // do not do this in generating ADAM output
    //if((alnTmp.flag & 0x100) > 0) {   // for secondary alignments, don't write SEQ and QUAL
    //  builder.setSequence("*")   // SEQ
    //  builder.setQual("*")       // QUAL
    //}
    //else if(alnTmp.isRev == 0) {   // the forward strand
    if(alnTmp.isRev == 0) {   // the forward strand
      var qb = 0
      var qe = seq.seqLength

      if(alnTmp.nCigar > 0) {
        if(which > 0 && (alnTmp.cigar.cigarSegs(0).op == 4 || alnTmp.cigar.cigarSegs(0).op == 3)) qb += alnTmp.cigar.cigarSegs(0).len
        if(which > 0 && (alnTmp.cigar.cigarSegs(alnTmp.nCigar - 1).op == 4 || alnTmp.cigar.cigarSegs(alnTmp.nCigar - 1).op == 3)) qe -= alnTmp.cigar.cigarSegs(alnTmp.nCigar - 1).len
      }

      var seqStr = new SAMString
      var i = qb
      while(i < qe) {
        seqStr.addChar(int2forward(seqTrans(i)))
        i += 1
      }
      builder.setSequence(seqStr.toString)   // SEQ
      // seq.quality is a Java ByteBuffer
      // When using decode(seq.quality), it is a destructive operation
      // Therefore, we need to create a copy of ByteBuffer for seq.quality
      val bb = ByteBuffer.wrap(seq.quality.array)
      val qual = Charset.forName("ISO-8859-1").decode(bb).array;
      var qualStr = new SAMString

      if(qual.size > 0) {
        var i = qb
        while(i < qe) {
          qualStr.addChar(qual(i))
          i += 1
        }
      }
      else qualStr.addChar('*')

      builder.setQual(qualStr.toString)  // QUAL
    }
    else {   // the reverse strand
      var qb = 0
      var qe = seq.seqLength
      
      if(alnTmp.nCigar > 0) {
        if(which > 0 && (alnTmp.cigar.cigarSegs(0).op == 4 || alnTmp.cigar.cigarSegs(0).op == 3)) qe -= alnTmp.cigar.cigarSegs(0).len
        if(which > 0 && (alnTmp.cigar.cigarSegs(alnTmp.nCigar - 1).op == 4 || alnTmp.cigar.cigarSegs(alnTmp.nCigar - 1).op == 3)) qb += alnTmp.cigar.cigarSegs(alnTmp.nCigar - 1).len
      }

      var seqStr = new SAMString
      var i = qe - 1
      while(i >= qb) {
        seqStr.addChar(int2reverse(seqTrans(i)))
        i -= 1
      }
      builder.setSequence(seqStr.toString)   // SEQ
      // seq.quality is a Java ByteBuffer
      // When using decode(seq.quality), it is a destructive operation
      // Therefore, we need to create a copy of ByteBuffer for seq.quality
      val bb = ByteBuffer.wrap(seq.quality.array)
      val qual = Charset.forName("ISO-8859-1").decode(bb).array;
      var qualStr = new SAMString

      if(qual.size > 0) {
        var i = qe - 1
        while(i >= qb) {
          qualStr.addChar(qual(i))
          i -= 1
        }
      }
      else qualStr.addChar('*')

      builder.setQual(qualStr.toString)  // QUAL
    }

    // print mismatchingPositions in the ADAM object
    if(alnTmp.nCigar > 0) 
      builder.setMismatchingPositions(alnTmp.cigar.cigarStr)

    // print optional tags
    var attrStr = new SAMString
    if(alnTmp.nCigar > 0) {
      attrStr.addCharArray("NM:i:".toCharArray)
      attrStr.addCharArray(alnTmp.NM.toString.toCharArray)
    }
    if(alnTmp.score >= 0) {
      attrStr.addCharArray("\tAS:i:".toCharArray)
      attrStr.addCharArray(alnTmp.score.toString.toCharArray)
    }
    if(alnTmp.sub >= 0) {
      attrStr.addCharArray("\tXS:i:".toCharArray)
      attrStr.addCharArray(alnTmp.sub.toString.toCharArray)
    }
    // Read group is read using SAMHeader class 
    if(samHeader.bwaReadGroupID != "") {
      attrStr.addCharArray("\tRG:Z:".toCharArray)
      attrStr.addCharArray(samHeader.bwaReadGroupID.toCharArray)
    }
    
    if((alnTmp.flag & 0x100) == 0) { // not multi-hit
      var i = 0
      var isBreak = false
      while(i < alnList.size && !isBreak) {
        if(i != which && (alnList(i).flag & 0x100) == 0) { 
          isBreak = true 
          i -= 1
        }
        i += 1
      }

      if(i < alnList.size) { // there are other primary hits; output them
        attrStr.addCharArray("\tSA:Z:".toCharArray)
        var j = 0
        while(j < alnList.size) {
          if(j != which && (alnList(j).flag & 0x100) == 0) { // proceed if: 1) different from the current; 2) not shadowed multi hit
            attrStr.addCharArray(bns.anns(alnList(j).rid).name.toCharArray)
            attrStr.addChar(',')
            attrStr.addCharArray((alnList(j).pos + 1).toString.toCharArray)
            attrStr.addChar(',')
            if(alnList(j).isRev == 0) attrStr.addChar('+')
            else attrStr.addChar('-')
            attrStr.addChar(',')
            
            var k = 0
            while(k < alnList(j).nCigar) {
              attrStr.addCharArray(alnList(j).cigar.cigarSegs(k).len.toString.toCharArray)
              attrStr.addChar(int2op(alnList(j).cigar.cigarSegs(k).op))
              k += 1
            }

            attrStr.addChar(',')
            attrStr.addCharArray(alnList(j).mapq.toString.toCharArray)
            attrStr.addChar(',')
            attrStr.addCharArray(alnList(j).NM.toString.toCharArray)
            attrStr.addChar(';')

          }
          j += 1
        }
      } 
    }

    // seq.comment is a Java ByteBuffer
    // When using decode(seq.comment), it is a destructive operation
    // Therefore, we need to create a copy of ByteBuffer for seq.comment
    val bbComment = ByteBuffer.wrap(seq.comment.array)
    val comment = Charset.forName("ISO-8859-1").decode(bbComment).array;
    if(comment.size > 0) {
      attrStr.addChar('\t')
      attrStr.addCharArray(comment)
    }
    
    builder.setAttributes(attrStr.toString)

    // Set read group
    // Note that the alignments are generated from aligner but not from an input SAM file. 
    // Therefore, we should have only one read group.
    builder.setRecordGroupName(readGroup.recordGroupName)
           .setRecordGroupSequencingCenter(readGroup.sequencingCenter.getOrElse(null))
           .setRecordGroupDescription(readGroup.description.getOrElse(null))
           .setRecordGroupFlowOrder(readGroup.flowOrder.getOrElse(null))
           .setRecordGroupKeySequence(readGroup.keySequence.getOrElse(null))
           .setRecordGroupLibrary(readGroup.library.getOrElse(null))
           .setRecordGroupPlatform(readGroup.platform.getOrElse(null))
           .setRecordGroupPlatformUnit(readGroup.platformUnit.getOrElse(null))
           .setRecordGroupSample(readGroup.sample)

    if(readGroup.runDateEpoch.getOrElse(null) != null)
      builder.setRecordGroupRunDateEpoch(readGroup.runDateEpoch.get)
    if(readGroup.predictedMedianInsertSize.getOrElse(null) != null)
      builder.setRecordGroupPredictedMedianInsertSize(readGroup.predictedMedianInsertSize.get)

    builder.build
  }
}


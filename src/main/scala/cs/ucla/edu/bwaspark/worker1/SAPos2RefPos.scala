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

import cs.ucla.edu.bwaspark.datatype._

//standalone object for transforming from position in suffix array
//to position in reference genome
object SAPos2RefPos {

  def occAux(y: Long, c: Int): Int = {

    //println("The previous y is: " + y)

    var res1 = 0l
    var res2 = 0l

    var tmp1 = 0l
    var tmp2 = 0l
    var tmp3 = 0l

    if ( (c & 2) != 0 ) tmp1 = y else tmp1 = ~y

    tmp1 = tmp1 >>> 1

    if ( (c & 1) != 0 ) tmp2 = y else tmp2 = ~y

    tmp3 = 0x5555555555555555l

    res1 = tmp1 & tmp2 & tmp3

    //println("The 1st result is: " + res1)

    tmp3 = 0x3333333333333333l

    tmp1 = (res1 & tmp3)

    tmp2 = (res1 >>> 2) & tmp3

    res2 = tmp1 + tmp2

    //println("The 2nd result is: " + res2)

    tmp3 = 0x0f0f0f0f0f0f0f0fl

    tmp1 = (res2 + (res2 >>> 4)) & tmp3

    tmp1 = tmp1 * 0x0101010101010101l

    //println("The return value is: " + (tmp1 >>> 56).toInt)

    (tmp1 >>> 56).toInt
  }



  def bwtOcc(bwt: BWTType, pos: Long, x: Long): Long = {
    var n: Long = 0l
    var k = pos

    //transform c from Long to ubyte_t?(Int instead)
    var c = (x & 0xffl).toInt

    if (k == bwt.seqLen) bwt.L2(c+1) - bwt.L2(c)
    else if (k == -1l) 0l //the original is (uint64_t)(-1)
    else {
      if (k >= bwt.primary) k = k - 1

      //println(k)
      //calculate new pointer position
      var newStartPoint = ((k >>> 7) << 4).toInt
      //println(c)

      n = (bwt.bwt(c * 2 + 1 + newStartPoint)).toLong
      n = n << 32
      n = n + (bwt.bwt(c * 2 + newStartPoint)).toLong
      //println ("The n in bwtOcc is: " + n)

      //jump to the start of the first bwt cell

      newStartPoint += 8 //size of Long: 8 bytes

      val occIntvMask = (1l << 7) - 1l

      var newEndPoint = newStartPoint + (((k >>> 5) - ((k & ~occIntvMask) >>> 5)) << 1).toInt
      //println((((k >>> 5) - ((k & ~occIntvMask) >>> 5)) << 1))
      //println("newStartPoint: " + newStartPoint + ", newEndPoint: " + newEndPoint + ", diff: " + (newEndPoint - newStartPoint))
      while (newStartPoint < newEndPoint) {
        n = n + occAux(((bwt.bwt(newStartPoint).toLong << 32) | (bwt.bwt(newStartPoint + 1).toLong << 32 >>> 32)), c)
        newStartPoint += 2
      }
      //println ("The n after loop is: " + n)
      //println (bwt.bwt(newStartPoint.toInt))
      //println (bwt.bwt(newStartPoint.toInt + 1))
      n += occAux(((bwt.bwt(newStartPoint).toLong << 32) | (bwt.bwt(newStartPoint + 1).toLong << 32 >>> 32)) & ~((1l << ((~k & 31) << 1)) - 1), c)
      //println (n)
      if (c == 0) n -= ~k & 31
      //println ("The final n is: " + n)
      n

    }
  }

  //compute inverse CSA
  def bwtInvPsi(bwt:BWTType, k: Long): Long = {
    var x: Long = if (k > bwt.primary) k - 1 else k

    //println("The x before bwt_B0 is " + x)
    var bwtBwt = bwt.bwt(((x >>> 7 << 4) + 8 + ((x & 0x7f) >>> 4)).toInt)
    //println(((x >>> 7 << 4) + 8 + ((x & 0x7f) >>> 4)))
    //println(bwtBwt)
    //println((((~x & 0xf) << 1) & 3))
    //println(bwtBwt >>> (((~x & 0xf) << 1) & 3))
    x = bwtBwt >>> (((~x & 0xf) << 1).toInt) & 3
    //println("The x for bwt L2 access is " + x)
    //println(bwt.L2(x.toInt))
    //println(bwtOcc(bwt, k, x))
    x = bwt.L2(x.toInt) + bwtOcc(bwt, k, x)
    //println("The x for return is " + x)
    if (k == bwt.primary) 0
    else x
  }

  def suffixArrayPos2ReferencePos(bwt: BWTType, k: Long /*uint64_t*/): Long /*uint64_t*/ = {
 
    //initialization
    var sa = 0l
    var mask = (bwt.saIntv - 1).toLong

    var pos = k

    while ( (pos & mask) != 0 ) {
      sa += 1l
      pos = bwtInvPsi(bwt, pos)
    }

    sa + bwt.sa((pos / bwt.saIntv).toInt)

  }
}

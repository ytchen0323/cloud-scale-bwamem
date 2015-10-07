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


package cs.ucla.edu.bwaspark.util

object LocusEncode {
  /**
    *  LocusEncode scheme in FASTQ for different types of reads (e.g. Illumina, SOLiD, and so on)
    */
  def locusEncode(locus: Char): Byte = {
    // Transforming from A/C/G/T to 0,1,2,3
    // For SOLiD reads, they have encoded with 0,1,2,3 already from the second position
    // Please refer to: https://en.wikipedia.org/wiki/FASTQ_format (Color space)
    locus match {
      case 'A' => 0
      case 'a' => 0
      case '0' => 0
      case 'C' => 1
      case 'c' => 1
      case '1' => 1
      case 'G' => 2
      case 'g' => 2
      case '2' => 2
      case 'T' => 3
      case 't' => 3
      case '3' => 3
      case '-' => 5
      case _ => 4
    }
  }

}


#! /usr/bin/perl

# *
# * Licensed to the Apache Software Foundation (ASF) under one or more
# * contributor license agreements.  See the NOTICE file distributed with
# * this work for additional information regarding copyright ownership.
# * The ASF licenses this file to You under the Apache License, Version 2.0
# * (the "License"); you may not use this file except in compliance with
# * the License.  You may obtain a copy of the License at
# *
# *    http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# *


# input SAM file
$sam = $ARGV[0];
$is_pair_end = $ARGV[1];
$aligned_num = 0;
$total_read_num = 0;
$prev_read = "";

open IN, $sam or die "cannot open $sam\n";

while(<IN>) {
  if($_ =~ /^([a-zA-Z0-9_\:\@\/]+)\t(\d+)/) {
    if($is_pair_end == 1) {
      $is_secondary = $2 & 0x100;
      
      if(($is_secondary == 0) || ($1 ne $prev_read)) {
        if(($2 & 0x4) == 0) {
          $aligned_num++;
        }
        $total_read_num++;
      }
   
      $prev_read = $1;
    }
    else {
      if(($1 ne $prev_read)) {
        if(($2 & 0x4) == 0) {
          $aligned_num++;
        }
        $total_read_num++;
      }
   
      $prev_read = $1;
    }
  }

}

close IN;

$percentage = ($aligned_num / $total_read_num) * 100;

print "Input file: " . $sam . "\n";
print "Total # of reads: " . $total_read_num . "\n";
print "Total # of aligned reads: " . $aligned_num . "\n";
print "Aligned (%): " . $percentage . "%\n";
 

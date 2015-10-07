#!/usr/bin/perl

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


$c_infile = "/home/ytchen/bwa/bwa-0.7.8/test.sam";
$scala_infile = "/home/ytchen/incubator/stable/bwa-spark-0.2.0/test.sam";

# Read the output file from C implementation
$c_read_idx = -1;

open CIN, $c_infile;

$cur_read_name = "";
$prev_read_name = "";
$aln_idx = 0;

while(<CIN>) {
  chomp;
  if($_ =~ /(\S+)\t(\w+)\t(\S+)\t(\S+)\t(\w+)\t(\w+)\t(\S+)\t(\w+)\t(\w+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)/) {
    $cur_read_name = $1;
    if($cur_read_name ne $prev_read_name) {
      $c_read_idx++;
      $prev_read_name = $cur_read_name;
      $aln_idx = 0;
    }
    $cread[$c_read_idx][$aln_idx][0] = $1;
    $cread[$c_read_idx][$aln_idx][1] = $2;
    $cread[$c_read_idx][$aln_idx][2] = $3;
    $cread[$c_read_idx][$aln_idx][3] = $4;
    $cread[$c_read_idx][$aln_idx][4] = $5;
    $cread[$c_read_idx][$aln_idx][5] = $6;
    $cread[$c_read_idx][$aln_idx][6] = $7;
    $cread[$c_read_idx][$aln_idx][7] = $8;
    $cread[$c_read_idx][$aln_idx][8] = $9;
    $cread[$c_read_idx][$aln_idx][9] = $10;
    $cread[$c_read_idx][$aln_idx][10] = $11;
    $cread[$c_read_idx][$aln_idx][11] = $12;
    $cread[$c_read_idx][$aln_idx][12] = $13;
    $cread[$c_read_idx][$aln_idx][13] = $14;
    $cread[$c_read_idx][$aln_idx][14] = $15;
    $cread_aln_num[$c_read_idx] = $aln_idx;
    $aln_idx++;
  }
}

close CIN;

$c_read_num = $c_read_idx + 1;


#for($i = 0; $i < $c_read_num; $i++) {
#  for($j = 0; $j < ($cread_aln_num[$i] + 1); $j++) {
#    print "Aln $j ";
#    for($k = 0; $k < 15; $k++) {
#      print "$cread[$i][$j][$k] ";
#    }
#    print "\n";
#  }
#}
#print "\n";


for($i = 0; $i < $c_read_num; $i++) {
  splice(@tmp);
  splice(@sorted_tmp);
  for($j = 0; $j < ($cread_aln_num[$i] + 1); $j++) {
    #print "Aln $j ";
    for($k = 0; $k < 15; $k++) {
      $tmp[$j][$k] = $cread[$i][$j][$k];
      #print "$tmp[$j][$k] ";
    }
    #print "\n";
  }

  @sorted_tmp = sort {$a->[3] <=> $b->[3] || $a->[2] cmp $b->[2]} @tmp;
  for($j = 0; $j < ($cread_aln_num[$i] + 1); $j++) {
    for($k = 0; $k < 15; $k++) {
      $cread_sorted[$i][$j][$k] = $sorted_tmp[$j][$k];
    }
  }
}

#for($i = 0; $i < $c_read_num; $i++) {
#  for($j = 0; $j < ($cread_aln_num[$i] + 1); $j++) {
#    print "Aln $j ";
#    for($k = 0; $k < 15; $k++) {
#      print "$cread_sorted[$i][$j][$k] ";
#    }
#    print "\n";
#  }
#}
#print "\n";

# Read the output file from Scala implementation

$scala_read_idx = -1;

$cur_read_name = "";
$prev_read_name = "";
$aln_idx = 0;

open SCALAIN, $scala_infile;

while(<SCALAIN>) {
  chomp;
  if($_ =~ /(\S+)\t(\w+)\t(\S+)\t(\S+)\t(\w+)\t(\w+)\t(\S+)\t(\w+)\t(\w+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)/) {
    $cur_read_name = $1;
    if($cur_read_name ne $prev_read_name) {
      $scala_read_idx++;
      $prev_read_name = $cur_read_name;
      $aln_idx = 0;
    }
    $scalaread[$scala_read_idx][$aln_idx][0] = $1;
    $scalaread[$scala_read_idx][$aln_idx][1] = $2;
    $scalaread[$scala_read_idx][$aln_idx][2] = $3;
    $scalaread[$scala_read_idx][$aln_idx][3] = $4;
    $scalaread[$scala_read_idx][$aln_idx][4] = $5;
    $scalaread[$scala_read_idx][$aln_idx][5] = $6;
    $scalaread[$scala_read_idx][$aln_idx][6] = $7;
    $scalaread[$scala_read_idx][$aln_idx][7] = $8;
    $scalaread[$scala_read_idx][$aln_idx][8] = $9;
    $scalaread[$scala_read_idx][$aln_idx][9] = $10;
    $scalaread[$scala_read_idx][$aln_idx][10] = $11;
    $scalaread[$scala_read_idx][$aln_idx][11] = $12;
    $scalaread[$scala_read_idx][$aln_idx][12] = $13;
    $scalaread[$scala_read_idx][$aln_idx][13] = $14;
    $scalaread[$scala_read_idx][$aln_idx][14] = $15;
    $scalaread_aln_num[$scala_read_idx] = $aln_idx;
    $aln_idx++;
  }
}

close SCALAIN;

$scala_read_num = $scala_read_idx + 1;

#for($i = 0; $i < $scala_read_num; $i++) {
#  for($j = 0; $j < ($scalaread_aln_num[$i] + 1); $j++) {
#    print "Aln $j ";
#    for($k = 0; $k < 15; $k++) {
#      print "$scalaread[$i][$j][$k] ";
#    }
#    print "\n";
#  }
#}

for($i = 0; $i < $scala_read_num; $i++) {
  splice(@tmp);
  splice(@sorted_tmp);
  for($j = 0; $j < ($scalaread_aln_num[$i] + 1); $j++) {
    #print "Aln $j ";
    for($k = 0; $k < 15; $k++) {
      $tmp[$j][$k] = $scalaread[$i][$j][$k];
      #print "$tmp[$j][$k] ";
    }
    #print "\n";
  }

  @sorted_tmp = sort {$a->[3] <=> $b->[3] || $a->[2] cmp $b->[2]} @tmp;
  for($j = 0; $j < ($scalaread_aln_num[$i] + 1); $j++) {
    for($k = 0; $k < 15; $k++) {
      $scalaread_sorted[$i][$j][$k] = $sorted_tmp[$j][$k];
    }
  }
}

#for($i = 0; $i < $scala_read_num; $i++) {
#  for($j = 0; $j < ($scalaread_aln_num[$i] + 1); $j++) {
#    print "Aln $j ";
#    for($k = 0; $k < 15; $k++) {
#      print "$scalaread_sorted[$i][$j][$k] ";
#    }
#    print "\n";
#  }
#}


# compare the reg number by read
$flag = 1;
for($i = 0; $i < $c_read_num; $i++) {
  if($cread_aln_num[$i] != $scalaread_aln_num[$i]) {
    print "[Read $i] Different # of regs: $cread_aln_num[$i] $scalaread_aln_num[$i]\n";
    $flag = 0;
  }
  #else {
  #  print ($i . " Num:" . $cread_aln_num[$i] . "\n");
  #}
}

if($flag == 1) {
  print "The number of regs of ALL reads are identical in both C and Scala implementation\n";
}

# compare the reg content by read
$allflag = 1;
for($i = 0; $i < $c_read_num; $i++) {
  print "Read $i\n";
  for($j = 0; $j < ($cread_aln_num[$i] + 1); $j++) {
    $inflag = 1;
    for($k = 0; $k < 15; $k++) {
      if($scalaread_sorted[$i][$j][$k] != $cread_sorted[$i][$j][$k]) {        
        $inflag = 0;
        $allflag = 0;
        last;
      }
    }

    if($inflag == 0) {
      print "[C]Aln $j ";
      for($k = 0; $k < 15; $k++) {
        print "$cread_sorted[$i][$j][$k] "
      }
      print "\n";
      print "[Scala]Aln $j ";
      for($k = 0; $k < 15; $k++) {
        print "$scalaread_sorted[$i][$j][$k] "
      }
      print "\n";
    }
  }
  
}

if($allflag == 1) {
  print "Worker2 passed!!!!!!!!\n";
}


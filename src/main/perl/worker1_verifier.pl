#!/usr/bin/perl

#
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
#



$c_infile = "/home/ytchen/bwa/bwa-0.7.8/log";
$scala_infile = "/home/ytchen/incubator/stable/bwa-spark-0.2.0/log";

# Read the output file from C implementation
$c_read_idx = -1;

open CIN, $c_infile;

while(<CIN>) {
  chomp;
  if($_ eq "#####") {
    $c_read_idx++;
  }
  elsif($_ =~ /^Reg (\d+)\((\d+), (\d+), (\d+), (\d+), (\d+), (\d+), (\d+), (\d+), (\d+), (\d+), (\d+), (\d+)\)/) {
    $cread[$c_read_idx][$1][0] = $2;
    $cread[$c_read_idx][$1][1] = $3;
    $cread[$c_read_idx][$1][2] = $4;
    $cread[$c_read_idx][$1][3] = $5;
    $cread[$c_read_idx][$1][4] = $6;
    $cread[$c_read_idx][$1][5] = $7;
    $cread[$c_read_idx][$1][6] = $8;
    $cread[$c_read_idx][$1][7] = $9;
    $cread[$c_read_idx][$1][8] = $10;
    $cread[$c_read_idx][$1][9] = $11;
    $cread[$c_read_idx][$1][10] = $12;
    $cread[$c_read_idx][$1][11] = $13;
    $cread_reg_num[$c_read_idx] = $1;
  }
}

close CIN;

$c_read_num = $c_read_idx + 1;


#for($i = 0; $i < $c_read_num; $i++) {
#  for($j = 0; $j < ($cread_reg_num[$i] + 1); $j++) {
#    print "Reg $j ";
#    for($k = 0; $k < 12; $k++) {
#      print "$cread[$i][$j][$k] ";
#    }
#    print "\n";
#  }
#}
#print "\n";


for($i = 0; $i < $c_read_num; $i++) {
  splice(@tmp);
  splice(@sorted_tmp);
  for($j = 0; $j < ($cread_reg_num[$i] + 1); $j++) {
    #print "Reg $j ";
    for($k = 0; $k < 12; $k++) {
      $tmp[$j][$k] = $cread[$i][$j][$k];
      #print "$tmp[$j][$k] ";
    }
    #print "\n";
  }

  @sorted_tmp = sort {$a->[0] <=> $b->[0]} @tmp;
  for($j = 0; $j < ($cread_reg_num[$i] + 1); $j++) {
    for($k = 0; $k < 12; $k++) {
      $cread_sorted[$i][$j][$k] = $sorted_tmp[$j][$k];
    }
  }
}

#for($i = 0; $i < $c_read_num; $i++) {
#  for($j = 0; $j < ($cread_reg_num[$i] + 1); $j++) {
#    print "Reg $j ";
#    for($k = 0; $k < 12; $k++) {
#      print "$cread_sorted[$i][$j][$k] ";
#    }
#    print "\n";
#  }
#}
#print "\n";

# Read the output file from Scala implementation

$scala_read_idx = -1;

open SCALAIN, $scala_infile;

while(<SCALAIN>) {
  chomp;
  if($_ eq "#####") {
    $scala_read_idx++;
  }
  elsif($_ =~ /^Reg (\d+)\((\d+), (\d+), (\d+), (\d+), (\d+), (\d+), (\d+), (\d+), (\d+), (\d+), (\d+), (\d+)\)/) {
    $scalaread[$scala_read_idx][$1][0] = $2;
    $scalaread[$scala_read_idx][$1][1] = $3;
    $scalaread[$scala_read_idx][$1][2] = $4;
    $scalaread[$scala_read_idx][$1][3] = $5;
    $scalaread[$scala_read_idx][$1][4] = $6;
    $scalaread[$scala_read_idx][$1][5] = $7;
    $scalaread[$scala_read_idx][$1][6] = $8;
    $scalaread[$scala_read_idx][$1][7] = $9;
    $scalaread[$scala_read_idx][$1][8] = $10;
    $scalaread[$scala_read_idx][$1][9] = $11;
    $scalaread[$scala_read_idx][$1][10] = $12;
    $scalaread[$scala_read_idx][$1][11] = $13;
    $scalaread_reg_num[$scala_read_idx] = $1;
  }
}

close SCALAIN;

$scala_read_num = $scala_read_idx + 1;

#for($i = 0; $i < $scala_read_num; $i++) {
#  for($j = 0; $j < ($scalaread_reg_num[$i] + 1); $j++) {
#    print "Reg $j ";
#    for($k = 0; $k < 12; $k++) {
#      print "$scalaread[$i][$j][$k] ";
#    }
#    print "\n";
#  }
#}

for($i = 0; $i < $scala_read_num; $i++) {
  splice(@tmp);
  splice(@sorted_tmp);
  for($j = 0; $j < ($scalaread_reg_num[$i] + 1); $j++) {
    #print "Reg $j ";
    for($k = 0; $k < 12; $k++) {
      $tmp[$j][$k] = $scalaread[$i][$j][$k];
      #print "$tmp[$j][$k] ";
    }
    #print "\n";
  }

  @sorted_tmp = sort {$a->[0] <=> $b->[0]} @tmp;
  for($j = 0; $j < ($scalaread_reg_num[$i] + 1); $j++) {
    for($k = 0; $k < 12; $k++) {
      $scalaread_sorted[$i][$j][$k] = $sorted_tmp[$j][$k];
    }
  }
}

#for($i = 0; $i < $scala_read_num; $i++) {
#  for($j = 0; $j < ($scalaread_reg_num[$i] + 1); $j++) {
#    print "Reg $j ";
#    for($k = 0; $k < 12; $k++) {
#      print "$scalaread_sorted[$i][$j][$k] ";
#    }
#    print "\n";
#  }
#}


# compare the reg number by read
$flag = 1;
for($i = 0; $i < $c_read_num; $i++) {
  if($cread_reg_num[$i] != $scalaread_reg_num[$i]) {
    print "[Read $i] Different # of regs: $cread_reg_num[$i] $scalaread_reg_num[$i]\n";
    $flag = 0;
  }
}

if($flag == 1) {
  print "The number of regs of ALL reads are identical in both C and Scala implementation\n";
}

# compare the reg content by read
$allflag = 1;
for($i = 0; $i < $c_read_num; $i++) {
  print "Read $i\n";
  for($j = 0; $j < ($cread_reg_num[$i] + 1); $j++) {
    $inflag = 1;
    for($k = 0; $k < 12; $k++) {
      if($scalaread_sorted[$i][$j][$k] != $cread_sorted[$i][$j][$k]) {        
        $inflag = 0;
        $allflag = 0;
        last;
      }
    }

    if($inflag == 0) {
      print "[C]Reg $j ";
      for($k = 0; $k < 12; $k++) {
        print "$cread_sorted[$i][$j][$k] "
      }
      print "; [Scala]Reg $j ";
      for($k = 0; $k < 12; $k++) {
        print "$scalaread_sorted[$i][$j][$k] "
      }
      print "\n";
    }
  }
  
}

if($allflag == 1) {
  print "Worker1 passed!!!!!!!!\n";
}


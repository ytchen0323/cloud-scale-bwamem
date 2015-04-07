#! /usr/bin/perl

$inFile = $ARGV[0];
$outFile = $ARGV[1];
$lineNum = $ARGV[2];

open IN, $inFile;
open OUT, ">$outFile";

$i = 0;

while(<IN>) {
   if($i < $lineNum) {
      print OUT $_;
   }
   else {
      last;
   }
   $i++;
}

close IN;
close OUT;

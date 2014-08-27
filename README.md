cloud-scale-bwamem-0.1.0
===============
Goal:
(1) Integrate pair-end + JNI implementation with previous BWA-Spark (0.3.1 version)
(2) Do the accuracy analysis and find the percentage of un-aligned reads
(3) Kryo serialization integration

Legacy: 
Achieved in bwa-spark-0.2.0:
(1) Worker1 is verified
(2) FASTQ RDD is done
(3) Worker1 performance has been tuned!
(4) Worker2 implementation is done before the output to SAM/ADAM format

Development NOTE:
(1) The order after sorting INFLUENCES the results. The result will be slightly different from the original C version.
    This occurs in MemChainToAlign(), MemSortAndDedup() and MemMarkPrimarySe().

# Used for debugging and results verification on small input

# store RDD
# single-end
#SPARK_DRIVER_MEMORY=40g /home/pengwei/spark-1.0.1_modified/bin/spark-submit --executor-memory 20g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 48 --master spark://Jc11:7077 --driver-java-options "-XX:+PrintFlagsFinal" /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar upload-fastq -bn 1000000 0 48 /home/hadoopmaster/genomics/InputFiles/HCC1954_1.fq hdfs://Jc11:9000/user/ytchen/data/whole_flow/all/HCC1954_1_1Mpart.fq

# run cloud-scale bwamem
# ADAM output
SPARK_DRIVER_MEMORY=24g /home/pengwei/spark-1.0.1_modified/bin/spark-submit --executor-memory 36g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 48 --master spark://Jc11:7077 --driver-java-options "-XX:+PrintFlagsFinal" /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar cs-bwamem -bfn 16 -bPSW 1 -sbatch 10 -bPSWJNI 1 -jniPath /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so -oChoice 2 -oPath hdfs://Jc11:9000/user/ytchen/data/whole_flow/all/HCC1954_1_1Mpart.adam 0 /home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta hdfs://Jc11:9000/user/ytchen/data/whole_flow/all/HCC1954_1_1Mpart.fq 2001

# Berkeley ADAM
#/home/ytchen/incubator/adam-adam-parent-0.14.0/bin/adam-submit --master spark://Jc11:7077 --driver-memory 20g --executor-memory 36g transform -coalesce 48 hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/output/HCC1954_1_10M-11M_reads_comp.sam hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/output/HCC1954_1_10M-11M_reads_comp.adam
#/home/ytchen/incubator/adam-adam-parent-0.14.0/bin/adam-submit --master spark://Jc11:7077 --driver-memory 20g --executor-memory 36g transform -sort_reads hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/output/HCC1954_1_10M-11M_reads.adam/0 hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/output/HCC1954_1_10M-11M_sorted.adam
#/home/ytchen/incubator/adam-adam-parent-0.14.0/bin/adam-submit --master spark://Jc11:7077 --driver-memory 20g --executor-memory 36g transform -sort_reads hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/output/HCC1954_1_10M-11M_reads_comp.adam hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/output/HCC1954_1_10M-11M_sorted_comp.adam
#/home/ytchen/incubator/adam-adam-parent-0.14.0/bin/adam-submit --master spark://Jc11:7077 --driver-memory 20g --executor-memory 36g print hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/output/HCC1954_1_10M-11M_sorted.adam -o HCC1954_1_10M-11M_sorted.adam
#/home/ytchen/incubator/adam-adam-parent-0.14.0/bin/adam-submit --master spark://Jc11:7077 --driver-memory 20g --executor-memory 36g print hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/output/HCC1954_1_10M-11M_sorted_comp.adam -o HCC1954_1_10M-11M_sorted_comp.adam

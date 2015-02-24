# Usage
SPARK_DRIVER_MEMORY=80g /cdsc_nfs/cdsc0/software/spark/spark-1.2.1/bin/spark-submit --executor-memory 48g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 280 --master spark://cdsc0.cs.ucla.edu:7077 --driver-java-options "-XX:+PrintFlagsFinal" /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar help

# store RDD
# pair-end
#SPARK_DRIVER_MEMORY=80g /cdsc_nfs/cdsc0/software/spark/spark-1.2.1/bin/spark-submit --executor-memory 40g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 280 --master spark://cdsc0.cs.ucla.edu:7077 --driver-java-options "-XX:+PrintFlagsFinal" /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar upload-fastq -bn 10000000 1 280 /hdfs2/data/InputFiles/HCC1954/HCC1954_1_100Mreads.fq /hdfs2/data/InputFiles/HCC1954/HCC1954_2_100Mreads.fq hdfs://cdsc0:9000/user/ytchen/data/pair-end/HCC1954_100Mreads.fq

# run cloud-scale bwamem
# SAM output
SPARK_DRIVER_MEMORY=80g /cdsc_nfs/cdsc0/software/spark/spark-1.2.1/bin/spark-submit --executor-memory 48g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 280 --master spark://cdsc0.cs.ucla.edu:7077 --driver-java-options "-XX:+PrintFlagsFinal" --conf spark.driver.maxResultSize=60g /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar cs-bwamem -bfn 20 -bPSW 1 -sbatch 10 -bPSWJNI 1 -jniPath /cdsc_nfs/cdsc0/shared_lib/jniNative.so -oChoice 1 -oPath HCC1954_100Mreads.sam 1 /hdfs2/data/ReferenceMetadata/human_g1k_v37.fasta hdfs://cdsc0:9000/user/ytchen/data/pair-end/HCC1954_100Mreads.fq 80

# ADAM output
#SPARK_DRIVER_MEMORY=24g /home/pengwei/spark-1.1.0/bin/spark-submit --executor-memory 36g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 48 --master spark://Jc11:7077 --driver-java-options "-XX:+PrintFlagsFinal" /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar cs-bwamem -bfn 1 -bPSW 1 -sbatch 10 -bPSWJNI 1 -jniPath /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so -oChoice 2 -oPath hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/output/test_reads.adam 1 /home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/test_reads.fq 1


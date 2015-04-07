# Usage
# SPARK_DRIVER_MEMORY=48g $SPARK_HOME/bin/spark-submit --executor-memory 48g \
#     --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 280 \
#     --master spark://localhost:7077 \
#     --driver-java-options "-XX:+PrintFlagsFinal" \
#     ${BWAMEM_HOME}/target/cloud-scale-bwamem-0.1.0-assembly.jar help

# store RDD
# pair-end
SPARK_DRIVER_MEMORY=48g $SPARK_HOME/bin/spark-submit --executor-memory 40g \
    --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 20 \
    --master spark://$(hostname):7077 \
    --driver-java-options "-XX:+PrintFlagsFinal" \
    ${BWAMEM_HOME}/target/cloud-scale-bwamem-0.1.0-assembly.jar upload-fastq \
    -bn 10000000 1 280 /scratch/jmg3/HCC1954_1_100Mreads.fq \
    /hdfs2/data/InputFiles/HCC1954/HCC1954_2_100Mreads.fq hdfs://cdsc0:9000/user/ytchen/data/pair-end/HCC1954_100Mreads.fq

# run cloud-scale bwamem
# SAM output
# SPARK_DRIVER_MEMORY=48g $SPARK_HOME/bin/spark-submit --executor-memory 48g \
#     --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 20 \
#     --master spark://$(hostname):7077 \
#     --driver-java-options "-XX:+PrintFlagsFinal" \
#     --conf spark.driver.maxResultSize=60g \
#     ${BWAMEM_HOME}/target/cloud-scale-bwamem-0.1.0-assembly.jar cs-bwamem \
#     -bfn 20 -bPSW 1 -sbatch 10 -bPSWJNI 1 \
#     -jniPath ${BWAMEM_HOME}/target/jniNative.so -oChoice 1 \
#     -oPath /scratch/jmg3/HCC1954_100Mreads.sam 1 \
#     /scratch/jmg3/ReferenceMetadata/human_g1k_v37.fasta \
#     hdfs://localhost:9000/HCC1954_1.fq 80

# ADAM output
#SPARK_DRIVER_MEMORY=24g /home/pengwei/spark-1.1.0/bin/spark-submit --executor-memory 36g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 48 --master spark://Jc11:7077 --driver-java-options "-XX:+PrintFlagsFinal" /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar cs-bwamem -bfn 1 -bPSW 1 -sbatch 10 -bPSWJNI 1 -jniPath /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so -oChoice 2 -oPath hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/output/test_reads.adam 1 /home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/test_reads.fq 1


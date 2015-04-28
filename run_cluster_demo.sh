SPARK_DRIVER_MEMORY=80g /cdsc_nfs/cdsc0/software/spark/spark-1.2.1/bin/spark-submit \
--executor-memory 54g \
--class cs.ucla.edu.bwaspark.BWAMEMSpark \
--total-executor-cores 24 \
--master spark://10.0.1.2:7077 \
--driver-java-options "-XX:+PrintFlagsFinal" \
--conf spark.driver.host=10.0.1.2 \
--conf spark.driver.maxResultSize=60g \
--conf spark.storage.memoryFraction=0.7 \
--conf spark.eventLog.enabled=true \
--conf spark.akka.threads=20 \
--conf spark.akka.frameSize=1024 \
/home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar cs-bwamem-profile -bfn 4 -bPSW 1 -sbatch 10 -bPSWJNI 1 -jniPath /cdsc_nfs/cdsc0/shared_lib/jniNative.so -oChoice 1 -oPath /hdfs2/data/InputFiles/HCC1954/HCC1954_100Mreads_test.sam -R "@RG	ID:HCC1954	LB:HCC1954	SM:HCC1954" -isSWExtBatched 1 -bSWExtSize 32768 -FPGAAccSWExt 1 -FPGASWExtThreshold 128 -jniSWExtendLibPath "/curr/genomics_spark/shared_lib/jniSWExtend.so" 1 /hdfs2/data/ReferenceMetadata/human_g1k_v37.fasta hdfs://cdsc0:9000/user/ytchen/data/SC_data/pair_end/HCC1954_100Mreads_tinyPart_SC.fq 8


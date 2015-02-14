# Used for debugging and results verification on small input

# remove files on HDFS
/usr/local/hadoop/hadoop-2.4.1/bin/hdfs dfs -rm -r hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/test_reads.fq

# store RDD
# pair-end
SPARK_DRIVER_MEMORY=40g /home/pengwei/spark-1.1.0/bin/spark-submit --executor-memory 20g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 48 --master spark://Jc11:7077 --driver-java-options "-XX:+PrintFlagsFinal" /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar upload-fastq -bn 8 1 48 test_1.fq test_2.fq hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/test_reads.fq

# remove files on HDFS
/usr/local/hadoop/hadoop-2.4.1/bin/hdfs dfs -rm -r hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/output/test_*

# run cloud-scale bwamem
# SAM output
SPARK_DRIVER_MEMORY=24g /home/pengwei/spark-1.1.0/bin/spark-submit --executor-memory 36g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 48 --master spark://Jc11:7077 --driver-java-options "-XX:+PrintFlagsFinal" /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar cs-bwamem -bfn 1 -bPSW 1 -sbatch 10 -bPSWJNI 1 -jniPath /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so -oChoice 1 -oPath test_reads_comp.sam 1 /home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/test_reads.fq 1

# ADAM output
SPARK_DRIVER_MEMORY=24g /home/pengwei/spark-1.1.0/bin/spark-submit --executor-memory 36g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 48 --master spark://Jc11:7077 --driver-java-options "-XX:+PrintFlagsFinal" /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar cs-bwamem -bfn 1 -bPSW 1 -sbatch 10 -bPSWJNI 1 -jniPath /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so -oChoice 2 -oPath hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/output/test_reads.adam 1 /home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/test_reads.fq 1

/usr/local/hadoop/hadoop-2.4.1/bin/hdfs dfs -copyFromLocal test_reads_comp.sam hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/output/test_reads_comp.sam

# Berkeley ADAM
# Figure out where ADAM is installed
ADAM_REPO="/home/ytchen/incubator/adam-adam-parent-0.14.0"

CLASSPATH=$("$ADAM_REPO"/bin/compute-adam-classpath.sh)
ADAM_JARS=$("$ADAM_REPO"/bin/compute-adam-jars.sh)

# Find the ADAM CLI jar
num_versions=$(ls "$ADAM_REPO"/adam-cli/target/appassembler/repo/org/bdgenomics/adam/adam-cli | wc -l)
if [ "$num_versions" -eq "0" ]; then
  echo "Failed to find adam-cli jar in $ADAM_REPO/adam-cli/target/appassembler/repo/org/bdgenomics/adam/adam-cli"
  echo "You need to build ADAM before running this program."
  exit 1
fi
if [ "$num_versions" -gt "1" ]; then
  versions_list=$(ls "$ADAM_REPO"/adam-cli/target/appassembler/repo/org/bdgenomics/adam/adam-cli)
  echo "Found multiple ADAM CLI versions in $ADAM_REPO/adam-cli/target/appassembler/repo/org/bdgenomics/adam/adam-cli:"
  echo "$versions_list"
  echo "Please remove all but one."
  exit 1
fi
ADAM_CLI_JAR=$(ls "$ADAM_REPO"/adam-cli/target/appassembler/repo/org/bdgenomics/adam/adam-cli/*/adam-cli-*.jar)

if [ -z "$SPARK_HOME" ]; then
  echo "Attempting to use 'spark-submit' on default path; you might need to set SPARK_HOME"
  SPARK_SUBMIT=spark-submit
else
  SPARK_SUBMIT="$SPARK_HOME"/bin/spark-submit
fi

SPARK_DRIVER_MEMORY=20g /home/pengwei/spark-1.1.0/bin/spark-submit --master spark://Jc11:7077 --driver-memory 20g --executor-memory 36g --total-executor-cores 48 --class org.bdgenomics.adam.cli.ADAMMain --properties-file "$ADAM_REPO"/bin/adam-spark-defaults.conf --jars "$ADAM_JARS" "$ADAM_CLI_JAR" transform -coalesce 48 hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/output/test_reads_comp.sam hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/output/test_reads_comp.adam

SPARK_DRIVER_MEMORY=20g /home/pengwei/spark-1.1.0/bin/spark-submit --master spark://Jc11:7077 --driver-memory 20g --executor-memory 36g --total-executor-cores 48 --class org.bdgenomics.adam.cli.ADAMMain --properties-file "$ADAM_REPO"/bin/adam-spark-defaults.conf --jars "$ADAM_JARS" "$ADAM_CLI_JAR" transform -sort_reads hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/output/test_reads.adam/0 hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/output/test_sorted.adam

SPARK_DRIVER_MEMORY=20g /home/pengwei/spark-1.1.0/bin/spark-submit --master spark://Jc11:7077 --driver-memory 20g --executor-memory 36g --total-executor-cores 48 --class org.bdgenomics.adam.cli.ADAMMain --properties-file "$ADAM_REPO"/bin/adam-spark-defaults.conf --jars "$ADAM_JARS" "$ADAM_CLI_JAR" transform -sort_reads hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/output/test_reads_comp.adam hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/output/test_sorted_comp.adam

SPARK_DRIVER_MEMORY=20g /home/pengwei/spark-1.1.0/bin/spark-submit --master spark://Jc11:7077 --driver-memory 20g --executor-memory 36g --total-executor-cores 48 --class org.bdgenomics.adam.cli.ADAMMain --properties-file "$ADAM_REPO"/bin/adam-spark-defaults.conf --jars "$ADAM_JARS" "$ADAM_CLI_JAR" print hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/output/test_reads.adam/0 -o test_reads.adam

SPARK_DRIVER_MEMORY=20g /home/pengwei/spark-1.1.0/bin/spark-submit --master spark://Jc11:7077 --driver-memory 20g --executor-memory 36g --total-executor-cores 48 --class org.bdgenomics.adam.cli.ADAMMain --properties-file "$ADAM_REPO"/bin/adam-spark-defaults.conf --jars "$ADAM_JARS" "$ADAM_CLI_JAR" print hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/output/test_reads_comp.adam -o test_reads_comp.adam

SPARK_DRIVER_MEMORY=20g /home/pengwei/spark-1.1.0/bin/spark-submit --master spark://Jc11:7077 --driver-memory 20g --executor-memory 36g --total-executor-cores 48 --class org.bdgenomics.adam.cli.ADAMMain --properties-file "$ADAM_REPO"/bin/adam-spark-defaults.conf --jars "$ADAM_JARS" "$ADAM_CLI_JAR" print hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/output/test_sorted.adam -o test_sorted.adam

SPARK_DRIVER_MEMORY=20g /home/pengwei/spark-1.1.0/bin/spark-submit --master spark://Jc11:7077 --driver-memory 20g --executor-memory 36g --total-executor-cores 48 --class org.bdgenomics.adam.cli.ADAMMain --properties-file "$ADAM_REPO"/bin/adam-spark-defaults.conf --jars "$ADAM_JARS" "$ADAM_CLI_JAR" print hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/output/test_sorted_comp.adam -o test_sorted_comp.adam

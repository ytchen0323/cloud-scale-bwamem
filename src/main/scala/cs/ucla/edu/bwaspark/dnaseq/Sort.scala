package cs.ucla.edu.bwaspark.dnaseq

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.rdd.read.ADAMAlignmentRecordContext
import org.bdgenomics.adam.rdd.read.ADAMAlignmentRecordContext._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.net.URI

object Sort extends Serializable {
  def apply(sc: SparkContext, hdfsAddress: String, alignmentsRootPath: String, coalesceFactor: Int) = {
    val conf = new Configuration
    val fs = FileSystem.get(new URI(hdfsAddress), conf)
    val paths = fs.listStatus(new Path(alignmentsRootPath)).map(ele => ele.getPath)
    val totalFilePartitions = paths.flatMap(p => fs.listStatus(p)).map(ele => ele.getPath).size
    println("Total number of new file partitions" + (totalFilePartitions/coalesceFactor))
    var adamRecords: RDD[AlignmentRecord] = new ADAMAlignmentRecordContext(sc).loadADAMFromPaths(paths)
    adamRecords.coalesce(totalFilePartitions/coalesceFactor).adamSortReadsByReferencePosition()
  }
}

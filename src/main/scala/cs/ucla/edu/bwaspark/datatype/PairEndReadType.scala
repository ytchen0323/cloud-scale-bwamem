package cs.ucla.edu.bwaspark.datatype

import scala.Serializable

import org.apache.avro.io._
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter

import cs.ucla.edu.avro.fastq._

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.ObjectStreamException

class PairEndReadType extends Serializable {
  var seq0: FASTQRecord = _
  var regs0: Array[MemAlnRegType] = _
  var seq1: FASTQRecord = _
  var regs1: Array[MemAlnRegType] = _

  private def writeObject(out: ObjectOutputStream) {
    out.writeObject(regs0)
    out.writeObject(regs1)
    val writer = new SpecificDatumWriter[FASTQRecord](classOf[FASTQRecord])
    val encoder = EncoderFactory.get.binaryEncoder(out, null)
    writer.write(seq0, encoder)
    writer.write(seq1, encoder)
    encoder.flush()
  }

  private def readObject(in: ObjectInputStream) {
    regs0 = in.readObject.asInstanceOf[Array[MemAlnRegType]]
    regs1 = in.readObject.asInstanceOf[Array[MemAlnRegType]]
    val reader = new SpecificDatumReader[FASTQRecord](classOf[FASTQRecord]);
    val decoder = DecoderFactory.get.binaryDecoder(in, null);
    seq0 = reader.read(null, decoder).asInstanceOf[FASTQRecord]
    seq1 = reader.read(null, decoder).asInstanceOf[FASTQRecord]
  }

  private def readObjectNoData() {

  }
}


package cs.ucla.edu.bwaspark.datatype

import scala.Serializable

import org.apache.avro.io._
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter

import cs.ucla.edu.avro.fastq._

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.ObjectStreamException

class ReadType extends Serializable {
  var seq: FASTQRecord = _
  var regs: Array[MemAlnRegType] = _

  private def writeObject(out: ObjectOutputStream) {
    out.writeObject(regs)
    val writer = new SpecificDatumWriter[FASTQRecord](classOf[FASTQRecord])
    val encoder = EncoderFactory.get.binaryEncoder(out, null)
    writer.write(seq, encoder)
    encoder.flush()
  }

  private def readObject(in: ObjectInputStream) {
    regs = in.readObject.asInstanceOf[Array[MemAlnRegType]]
    val reader = new SpecificDatumReader[FASTQRecord](classOf[FASTQRecord]);
    val decoder = DecoderFactory.get.binaryDecoder(in, null);
    seq = reader.read(null, decoder).asInstanceOf[FASTQRecord]
  }

  private def readObjectNoData() {

  }
}


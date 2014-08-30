package cs.ucla.edu.bwaspark.fastq;

import cs.ucla.edu.avro.fastq.*;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 *   For now, Spark does not support Avro. This class is just a quick
 *   workaround that (de)serializes PairEndFASTQRecord objects using Avro.
 */
public class SerializablePairEndFASTQRecord extends PairEndFASTQRecord implements Serializable {

    private void setValues(PairEndFASTQRecord rec) {
        setSeq0(rec.getSeq0());
        setSeq1(rec.getSeq1());
    }

    public SerializablePairEndFASTQRecord(PairEndFASTQRecord rec) {
        setValues(rec);
    }

    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException {
        DatumWriter<PairEndFASTQRecord> writer = new SpecificDatumWriter<PairEndFASTQRecord>(PairEndFASTQRecord.class);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(this, encoder);
        encoder.flush();
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        DatumReader<PairEndFASTQRecord> reader =
                new SpecificDatumReader<PairEndFASTQRecord>(PairEndFASTQRecord.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        setValues(reader.read(null, decoder));
    }

    private void readObjectNoData()
            throws ObjectStreamException {
    }

}


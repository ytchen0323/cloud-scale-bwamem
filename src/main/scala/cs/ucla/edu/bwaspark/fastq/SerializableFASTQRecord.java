/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


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
 *   workaround that (de)serializes FASTQRecord objects using Avro.
 */
public class SerializableFASTQRecord extends FASTQRecord implements Serializable {

    private void setValues(FASTQRecord rec) {
        setName(rec.getName());
        setSeq(rec.getSeq());
        setSeqLength(rec.getSeqLength());
        setQuality(rec.getQuality());
        setComment(rec.getComment());
    }

    public SerializableFASTQRecord(FASTQRecord rec) {
        setValues(rec);
    }

    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException {
        DatumWriter<FASTQRecord> writer = new SpecificDatumWriter<FASTQRecord>(FASTQRecord.class);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(this, encoder);
        encoder.flush();
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        DatumReader<FASTQRecord> reader =
                new SpecificDatumReader<FASTQRecord>(FASTQRecord.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        setValues(reader.read(null, decoder));
    }

    private void readObjectNoData()
            throws ObjectStreamException {
    }

}


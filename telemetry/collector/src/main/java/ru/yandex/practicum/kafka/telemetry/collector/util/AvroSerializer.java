package ru.yandex.practicum.kafka.telemetry.collector.util;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Component
public class AvroSerializer {

    public byte[] serialize(SpecificRecordBase record) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
            DatumWriter<SpecificRecordBase> writer = new SpecificDatumWriter<>(record.getSchema());
            writer.write(record, encoder);
            encoder.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Avro record: " + record.getClass().getSimpleName(), e);
        }
    }
}

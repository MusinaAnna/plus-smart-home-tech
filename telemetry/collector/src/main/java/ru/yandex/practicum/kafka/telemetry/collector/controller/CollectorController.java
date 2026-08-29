package ru.yandex.practicum.kafka.telemetry.collector.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.kafka.telemetry.collector.dto.HubEvent;
import ru.yandex.practicum.kafka.telemetry.collector.dto.SensorEvent;
import ru.yandex.practicum.kafka.telemetry.collector.mapper.HubEventMapper;
import ru.yandex.practicum.kafka.telemetry.collector.mapper.SensorEventMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@RestController
@RequestMapping("/events")
@RequiredArgsConstructor
public class CollectorController {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final SensorEventMapper sensorEventMapper;
    private final HubEventMapper hubEventMapper;

    private static final String SENSORS_TOPIC = "telemetry.sensors.v1";
    private static final String HUBS_TOPIC = "telemetry.hubs.v1";

    @PostMapping("/sensors")
    public ResponseEntity<Void> collectSensorEvent(@Valid @RequestBody SensorEvent event) throws IOException {
        SpecificRecord avroEvent = sensorEventMapper.toAvro(event);
        byte[] data = serializeAvro(avroEvent);
        kafkaTemplate.send(SENSORS_TOPIC, event.getHubId(), data);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/hubs")
    public ResponseEntity<Void> collectHubEvent(@Valid @RequestBody HubEvent event) throws IOException {
        SpecificRecord avroEvent = hubEventMapper.toAvro(event);
        byte[] data = serializeAvro(avroEvent);
        kafkaTemplate.send(HUBS_TOPIC, event.getHubId(), data);
        return ResponseEntity.ok().build();
    }

    private byte[] serializeAvro(SpecificRecord record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        SpecificDatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(record.getSchema());
        writer.write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }
}

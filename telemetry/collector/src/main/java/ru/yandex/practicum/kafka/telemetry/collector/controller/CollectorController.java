package ru.yandex.practicum.kafka.telemetry.collector.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.kafka.telemetry.collector.dto.HubEvent;
import ru.yandex.practicum.kafka.telemetry.collector.dto.SensorEvent;
import ru.yandex.practicum.kafka.telemetry.collector.mapper.HubEventMapper;
import ru.yandex.practicum.kafka.telemetry.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.kafka.telemetry.collector.util.AvroSerializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@RestController
public class CollectorController {

    private static final Logger log = LoggerFactory.getLogger(CollectorController.class);
    private static final String HUBS_TOPIC = "telemetry.hubs.v1";
    private static final String SENSORS_TOPIC = "telemetry.sensors.v1";

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final SensorEventMapper sensorEventMapper;
    private final HubEventMapper hubEventMapper;
    private final AvroSerializer avroSerializer;

    public CollectorController(KafkaTemplate<String, byte[]> kafkaTemplate,
                               SensorEventMapper sensorEventMapper,
                               HubEventMapper hubEventMapper,
                               AvroSerializer avroSerializer) {
        this.kafkaTemplate = kafkaTemplate;
        this.sensorEventMapper = sensorEventMapper;
        this.hubEventMapper = hubEventMapper;
        this.avroSerializer = avroSerializer;
    }

    @PostMapping("/events/sensors")
    public ResponseEntity<Void> handleSensorEvent(@RequestBody SensorEvent event) {
        log.debug("Received sensor event: {}", event);
        SensorEventAvro avroEvent = sensorEventMapper.toAvro(event);
        byte[] avroBytes = avroSerializer.serialize(avroEvent);
        kafkaTemplate.send(SENSORS_TOPIC, event.getHubId(), avroBytes);
        log.info("Sensor event sent to Kafka: id={}, hubId={}", event.getId(), event.getHubId());
        return ResponseEntity.ok().build();
    }

    @PostMapping("/events/hubs")
    public ResponseEntity<Void> handleHubEvent(@RequestBody HubEvent event) {
        log.debug("Received hub event: {}", event);
        HubEventAvro avroEvent = hubEventMapper.toAvro(event);
        byte[] avroBytes = avroSerializer.serialize(avroEvent);
        kafkaTemplate.send(HUBS_TOPIC, event.getHubId(), avroBytes);
        log.info("Hub event sent to Kafka: hubId={}", event.getHubId());
        return ResponseEntity.ok().build();
    }
}

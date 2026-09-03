package ru.yandex.practicum.kafka.telemetry.collector.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.kafka.telemetry.collector.dto.HubEvent;
import ru.yandex.practicum.kafka.telemetry.collector.dto.SensorEvent;
import ru.yandex.practicum.kafka.telemetry.collector.mapper.HubEventMapper;
import ru.yandex.practicum.kafka.telemetry.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.kafka.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.util.AvroSerializer;

@Slf4j
@RestController
public class CollectorController {

    private final SensorEventMapper sensorEventMapper;
    private final HubEventMapper hubEventMapper;
    private final AvroSerializer avroSerializer;
    private final KafkaEventProducer producer;

    @Autowired
    public CollectorController(SensorEventMapper sensorEventMapper,
                               HubEventMapper hubEventMapper,
                               AvroSerializer avroSerializer,
                               KafkaEventProducer producer) {
        this.sensorEventMapper = sensorEventMapper;
        this.hubEventMapper = hubEventMapper;
        this.avroSerializer = avroSerializer;
        this.producer = producer;
    }

    @PostMapping("/events/sensors")
    public ResponseEntity<Void> handleSensorEvent(@RequestBody SensorEvent event) {
        log.info("Received sensor event: {}", event);

        SensorEventAvro avroEvent = sensorEventMapper.toAvro(event);

        byte[] avroBytes = avroSerializer.serialize(avroEvent);

        producer.sendSensorEvent(avroEvent, event.getHubId(), avroBytes);

        log.info("Sensor event sent to Kafka: id={}, hubId={}", event.getId(), event.getHubId());
        return ResponseEntity.ok().build();
    }

    @PostMapping("/events/hubs")
    public ResponseEntity<Void> handleHubEvent(@RequestBody HubEvent event) {
        log.info("Received hub event: {}", event);

        HubEventAvro avroEvent = hubEventMapper.toAvro(event);

        byte[] avroBytes = avroSerializer.serialize(avroEvent);

        producer.sendHubEvent(avroEvent, event.getHubId(), avroBytes);

        log.info("Hub event sent to Kafka: hubId={}", event.getHubId());
        return ResponseEntity.ok().build();
    }
}

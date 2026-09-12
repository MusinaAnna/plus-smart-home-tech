package ru.yandex.practicum.kafka.telemetry.collector.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
@Component
public class KafkaEventProducer implements AutoCloseable {

    private static final String SENSORS_TOPIC = "telemetry.sensors.v1";
    private static final String HUBS_TOPIC = "telemetry.hubs.v1";

    private final KafkaProducer<String, byte[]> producer;

    public KafkaEventProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        this.producer = new KafkaProducer<>(props);
        log.info("KafkaProducer инициализирован для сервера {}", "localhost:9092");
    }

    public void sendSensorEvent(SpecificRecordBase event, String hubId, byte[] avroBytes) {
        send(SENSORS_TOPIC, hubId, avroBytes, event.getClass().getSimpleName());
    }

    public void sendHubEvent(SpecificRecordBase event, String hubId, byte[] avroBytes) {
        send(HUBS_TOPIC, hubId, avroBytes, event.getClass().getSimpleName());
    }

    private void send(String topic, String hubId, byte[] avroBytes, String eventClass) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, hubId, avroBytes);

        log.debug("Отправка события {} для хаба {} в топик {}", eventClass, hubId, topic);

        try {
            Future<RecordMetadata> future = producer.send(record);
            producer.flush();
            RecordMetadata metadata = future.get();
            log.info("Событие {} сохранено в топик {} (партиция {}, смещение {})",
                    eventClass, metadata.topic(), metadata.partition(), metadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            log.error("Не удалось отправить событие {} в топик {}", eventClass, topic, e);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    @PreDestroy
    public void close() {
        log.info("Закрытие KafkaProducer...");
        producer.flush();
        producer.close();
        log.info("KafkaProducer закрыт");
    }
}

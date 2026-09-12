package ru.yandex.practicum.kafka.telemetry.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.util.AvroSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private static final String SENSORS_TOPIC = "telemetry.sensors.v1";
    private static final String SNAPSHOTS_TOPIC = "telemetry.snapshots.v1";

    private final KafkaConsumer<String, SensorEventAvro> consumer;
    private final KafkaProducer<String, byte[]> producer;
    private final SensorsSnapshotAggregator aggregator;
    private final AvroSerializer avroSerializer;

    public void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(List.of(SENSORS_TOPIC));

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records =
                        consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    Optional<SensorsSnapshotAvro> snapshot =
                            aggregator.updateState(record.value());

                    if (snapshot.isPresent()) {
                        SensorsSnapshotAvro updatedSnapshot = snapshot.get();

                        producer.send(new ProducerRecord<>(
                                SNAPSHOTS_TOPIC,
                                updatedSnapshot.getHubId(),
                                avroSerializer.serialize(updatedSnapshot)
                        ));
                    }
                }

                consumer.commitSync();
            }

        } catch (WakeupException ignored) {
            // Завершаем работу в блоке finally
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {
            try {
                producer.flush();
                consumer.commitSync();
            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();

                log.info("Закрываем продюсер");
                producer.close();
            }
        }
    }
}

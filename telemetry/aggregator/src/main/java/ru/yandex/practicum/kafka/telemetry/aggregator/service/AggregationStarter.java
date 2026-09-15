package ru.yandex.practicum.kafka.telemetry.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
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

    private final KafkaConsumer<String, SensorEventAvro> consumer;
    private final KafkaProducer<String, byte[]> producer;
    private final SensorsSnapshotAggregator aggregator;
    private final AvroSerializer avroSerializer;

    @Value("${aggregator.kafka.consumer.topic}")
    private String sensorsTopic;

    @Value("${aggregator.kafka.producer.topic}")
    private String snapshotsTopic;

    public void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(List.of(sensorsTopic));

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records =
                        consumer.poll(Duration.ofSeconds(1));

                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, SensorEventAvro> record : records) {
                        Optional<SensorsSnapshotAvro> snapshot =
                                aggregator.updateState(record.value());

                        if (snapshot.isPresent()) {
                            SensorsSnapshotAvro updatedSnapshot = snapshot.get();

                            producer.send(new ProducerRecord<>(
                                    snapshotsTopic,
                                    updatedSnapshot.getHubId(),
                                    avroSerializer.serialize(updatedSnapshot)
                            ));
                        }
                    }

                    consumer.commitSync();
                }
            }

        } catch (WakeupException ignored) {
            // Завершаем работу в блоке finally
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {
            producer.flush();

            log.info("Закрываем консьюмер");
            consumer.close();

            log.info("Закрываем продюсер");
            producer.close();
        }
    }
}

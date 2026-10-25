package ru.yandex.practicum.kafka.telemetry.aggregator.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.util.AvroSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AggregationStarterTest {

    @Mock
    private KafkaConsumer<String, SensorEventAvro> consumer;

    @Mock
    private KafkaProducer<String, byte[]> producer;

    @Mock
    private SensorsSnapshotAggregator aggregator;

    @Mock
    private AvroSerializer avroSerializer;

    @Mock
    private Future<RecordMetadata> confirmation;

    private AggregationStarter starter;

    @BeforeEach
    void setUp() {
        starter = new AggregationStarter(consumer, producer, aggregator, avroSerializer);
        ReflectionTestUtils.setField(starter, "sensorsTopic", "sensor-topic");
        ReflectionTestUtils.setField(starter, "snapshotsTopic", "snapshot-topic");
    }

    @Test
    void commitsOnlyAfterProducerConfirmation() throws Exception {
        prepareSingleSnapshot();
        when(confirmation.get()).thenAnswer(invocation -> {
            verify(consumer, never()).commitSync();
            return null;
        });

        starter.start();

        verify(confirmation).get();
        verify(consumer).commitSync();
        verify(producer).close();
        verify(consumer).close();
    }

    @Test
    void doesNotCommitWhenProducerConfirmationFails() throws Exception {
        prepareSingleSnapshot();
        when(confirmation.get()).thenThrow(
                new ExecutionException(new IllegalStateException("Kafka broker error"))
        );

        starter.start();

        verify(confirmation).get();
        verify(consumer, never()).commitSync();
        verify(producer).close();
        verify(consumer).close();
    }

    @Test
    void doesNotSendOrCommitWhenPollIsEmpty() {
        when(consumer.poll(any(Duration.class)))
                .thenReturn(ConsumerRecords.empty())
                .thenThrow(new WakeupException());

        starter.start();

        verify(producer, never()).send(any(ProducerRecord.class));
        verify(consumer, never()).commitSync();
        verify(consumer).close();
    }

    @Test
    void closesConsumerAndProducerEvenIfFlushFails() {
        when(consumer.poll(any(Duration.class))).thenThrow(new WakeupException());
        doThrow(new IllegalStateException("flush failure")).when(producer).flush();

        IllegalStateException failure = assertThrows(IllegalStateException.class, starter::start);

        assertEquals("flush failure", failure.getMessage());
        verify(producer).flush();
        verify(consumer).close();
        verify(producer).close();
        verify(consumer, never()).commitSync();
    }

    @Test
    void closesProducerEvenIfConsumerCloseFails() {
        when(consumer.poll(any(Duration.class))).thenThrow(new WakeupException());
        doThrow(new IllegalStateException("consumer close failure")).when(consumer).close();

        IllegalStateException failure = assertThrows(IllegalStateException.class, starter::start);

        assertEquals("consumer close failure", failure.getMessage());
        verify(producer).flush();
        verify(consumer).close();
        verify(producer).close();
        verify(consumer, never()).commitSync();
    }

    @SuppressWarnings("unchecked")
    private void prepareSingleSnapshot() {
        SensorEventAvro event = mock(SensorEventAvro.class);
        SensorsSnapshotAvro snapshot = mock(SensorsSnapshotAvro.class);

        ConsumerRecord<String, SensorEventAvro> record =
                new ConsumerRecord<>("sensor-topic", 0, 0L, "hub-1", event);
        ConsumerRecords<String, SensorEventAvro> batch = new ConsumerRecords<>(
                Map.of(new TopicPartition("sensor-topic", 0), List.of(record))
        );

        when(consumer.poll(any(Duration.class)))
                .thenReturn(batch)
                .thenThrow(new WakeupException());
        when(aggregator.updateState(event)).thenReturn(Optional.of(snapshot));
        when(snapshot.getHubId()).thenReturn("hub-1");
        when(avroSerializer.serialize(snapshot)).thenReturn(new byte[]{1, 2, 3});
        when(producer.send(any(ProducerRecord.class))).thenReturn(confirmation);
    }
}

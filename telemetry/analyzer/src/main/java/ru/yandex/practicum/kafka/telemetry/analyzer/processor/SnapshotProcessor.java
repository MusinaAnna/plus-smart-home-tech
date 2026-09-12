package ru.yandex.practicum.kafka.telemetry.analyzer.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.kafka.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.kafka.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.kafka.telemetry.analyzer.model.Sensor;
import net.devh.boot.grpc.client.inject.GrpcClient;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc.HubRouterControllerBlockingStub;
import com.google.protobuf.Timestamp;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.kafka.telemetry.analyzer.model.Action;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
public class SnapshotProcessor {

    private final KafkaConsumer<String, SensorsSnapshotAvro> consumer;
    private final ScenarioRepository scenarioRepository;
    private final HubRouterControllerBlockingStub hubRouterClient;

    @Value("${analyzer.kafka.snapshot-consumer.topic}")
    private String topic;

    public SnapshotProcessor(
            @Qualifier("snapshotConsumer")
            KafkaConsumer<String, SensorsSnapshotAvro> consumer,
            ScenarioRepository scenarioRepository,
            @GrpcClient("hub-router")
            HubRouterControllerBlockingStub hubRouterClient) {
        this.consumer = consumer;
        this.scenarioRepository = scenarioRepository;
        this.hubRouterClient = hubRouterClient;
    }

    public void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(List.of(topic));

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records =
                        consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    processSnapshot(record.value());
                }

                consumer.commitSync();
            }
        } catch (WakeupException ignored) {
            // Завершаем работу
        } finally {
            consumer.close();
        }
    }

    private void processSnapshot(SensorsSnapshotAvro snapshot) {
        scenarioRepository.findByHubId(snapshot.getHubId())
                .forEach(scenario -> {
                    if (checkScenario(scenario, snapshot)) {
                        executeScenario(scenario, snapshot);
                    }
                });
    }
    private Integer getSensorValue(Condition condition, SensorStateAvro state) {
        Object data = state.getData();

        return switch (condition.getType()) {
            case "MOTION" -> {
                if (data instanceof MotionSensorAvro motionSensor) {
                    yield motionSensor.getMotion() ? 1 : 0;
                }
                yield null;
            }

            case "LUMINOSITY" -> {
                if (data instanceof LightSensorAvro lightSensor) {
                    yield lightSensor.getLuminosity();
                }
                yield null;
            }

            case "SWITCH" -> {
                if (data instanceof SwitchSensorAvro switchSensor) {
                    yield switchSensor.getState() ? 1 : 0;
                }
                yield null;
            }

            case "TEMPERATURE" -> {
                if (data instanceof TemperatureSensorAvro temperatureSensor) {
                    yield temperatureSensor.getTemperatureC();
                }

                if (data instanceof ClimateSensorAvro climateSensor) {
                    yield climateSensor.getTemperatureC();
                }

                yield null;
            }

            case "CO2_LEVEL" -> {
                if (data instanceof ClimateSensorAvro climateSensor) {
                    yield climateSensor.getCo2Level();
                }
                yield null;
            }

            case "HUMIDITY" -> {
                if (data instanceof ClimateSensorAvro climateSensor) {
                    yield climateSensor.getHumidity();
                }
                yield null;
            }

            default -> null;
        };
    }
    private boolean checkCondition(Condition condition, SensorStateAvro state) {
        Integer sensorValue = getSensorValue(condition, state);

        if (sensorValue == null) {
            return false;
        }

        return switch (condition.getOperation()) {
            case "EQUALS" -> sensorValue.equals(condition.getValue());
            case "GREATER_THAN" -> sensorValue > condition.getValue();
            case "LOWER_THAN" -> sensorValue < condition.getValue();
            default -> false;
        };
    }
    private boolean checkScenario(Scenario scenario, SensorsSnapshotAvro snapshot) {
        for (var entry : scenario.getConditions().entrySet()) {
            Sensor sensor = entry.getKey();
            Condition condition = entry.getValue();

            SensorStateAvro state = snapshot.getSensorsState().get(sensor.getId());

            if (state == null || !checkCondition(condition, state)) {
                return false;
            }
        }

        return true;
    }
    private void executeScenario(Scenario scenario, SensorsSnapshotAvro snapshot) {
        for (var entry : scenario.getActions().entrySet()) {
            Sensor sensor = entry.getKey();
            Action action = entry.getValue();

            DeviceActionProto.Builder actionBuilder = DeviceActionProto.newBuilder()
                    .setSensorId(sensor.getId())
                    .setType(ActionTypeProto.valueOf(action.getType()));

            if (action.getValue() != null) {
                actionBuilder.setValue(action.getValue());
            }

            Timestamp timestamp = Timestamp.newBuilder()
                    .setSeconds(snapshot.getTimestamp().getEpochSecond())
                    .setNanos(snapshot.getTimestamp().getNano())
                    .build();

            DeviceActionRequest request = DeviceActionRequest.newBuilder()
                    .setHubId(snapshot.getHubId())
                    .setScenarioName(scenario.getName())
                    .setAction(actionBuilder.build())
                    .setTimestamp(timestamp)
                    .build();

            hubRouterClient.handleDeviceAction(request);
        }
    }
}

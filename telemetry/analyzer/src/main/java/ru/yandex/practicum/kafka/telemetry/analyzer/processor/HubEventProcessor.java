package ru.yandex.practicum.kafka.telemetry.analyzer.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.kafka.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.kafka.telemetry.analyzer.model.Sensor;
import ru.yandex.practicum.kafka.telemetry.analyzer.repository.ActionRepository;
import ru.yandex.practicum.kafka.telemetry.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.kafka.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.kafka.telemetry.analyzer.repository.SensorRepository;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.analyzer.model.Action;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;

@Slf4j
@Component
public class HubEventProcessor implements Runnable {

    private final KafkaConsumer<String, HubEventAvro> consumer;
    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;

    @Value("${analyzer.kafka.hub-consumer.topic}")
    private String topic;

    public HubEventProcessor(
            @Qualifier("hubEventConsumer")
            KafkaConsumer<String, HubEventAvro> consumer,
            SensorRepository sensorRepository,
            ScenarioRepository scenarioRepository,
            ConditionRepository conditionRepository,
            ActionRepository actionRepository) {
        this.consumer = consumer;
        this.sensorRepository = sensorRepository;
        this.scenarioRepository = scenarioRepository;
        this.conditionRepository = conditionRepository;
        this.actionRepository = actionRepository;
    }

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(List.of(topic));

            while (true) {
                ConsumerRecords<String, HubEventAvro> records =
                        consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    processEvent(record.value());
                }

                consumer.commitSync();
            }
        } catch (WakeupException ignored) {
            // Завершаем работу
        } finally {
            consumer.close();
        }
    }

    private void processEvent(HubEventAvro event) {
        Object payload = event.getPayload();

        if (payload instanceof DeviceAddedEventAvro deviceAddedEvent) {
            if (!sensorRepository.existsById(deviceAddedEvent.getId())) {
                Sensor sensor = new Sensor(
                        deviceAddedEvent.getId(),
                        event.getHubId()
                );

                sensorRepository.save(sensor);
            }

        } else if (payload instanceof DeviceRemovedEventAvro deviceRemovedEvent) {
            sensorRepository
                    .findByIdAndHubId(deviceRemovedEvent.getId(), event.getHubId())
                    .ifPresent(sensorRepository::delete);

        } else if (payload instanceof ScenarioAddedEventAvro scenarioAddedEvent) {
            Scenario scenario = scenarioRepository
                    .findByHubIdAndName(event.getHubId(), scenarioAddedEvent.getName())
                    .orElse(
                            new Scenario(
                                    null,
                                    event.getHubId(),
                                    scenarioAddedEvent.getName(),
                                    new HashMap<>(),
                                    new HashMap<>()
                            )
                    );

            scenario.getConditions().clear();

            for (ScenarioConditionAvro conditionAvro : scenarioAddedEvent.getConditions()) {
                Sensor sensor = sensorRepository
                        .findByIdAndHubId(conditionAvro.getSensorId(), event.getHubId())
                        .orElseThrow();

                Integer value;

                if (conditionAvro.getValue() instanceof Boolean boolValue) {
                    value = boolValue ? 1 : 0;
                } else {
                    value = (Integer) conditionAvro.getValue();
                }

                Condition condition = new Condition(
                        null,
                        conditionAvro.getType().name(),
                        conditionAvro.getOperation().name(),
                        value
                );

                conditionRepository.save(condition);
                scenario.getConditions().put(sensor, condition);
            }

            scenario.getActions().clear();

            for (DeviceActionAvro actionAvro : scenarioAddedEvent.getActions()) {
                Sensor sensor = sensorRepository
                        .findByIdAndHubId(actionAvro.getSensorId(), event.getHubId())
                        .orElseThrow();

                Action action = new Action(
                        null,
                        actionAvro.getType().name(),
                        (Integer) actionAvro.getValue()
                );

                actionRepository.save(action);
                scenario.getActions().put(sensor, action);
            }

            scenarioRepository.save(scenario);

        } else if (payload instanceof ScenarioRemovedEventAvro scenarioRemovedEvent) {
            scenarioRepository
                    .findByHubIdAndName(event.getHubId(), scenarioRemovedEvent.getName())
                    .ifPresent(scenarioRepository::delete);
        }
    }
}

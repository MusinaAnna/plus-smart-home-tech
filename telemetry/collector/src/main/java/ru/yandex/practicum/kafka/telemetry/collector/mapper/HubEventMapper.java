package ru.yandex.practicum.kafka.telemetry.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.util.stream.Collectors;

@Component
public class HubEventMapper {

    public HubEventAvro toAvro(HubEventProto event) {
        Instant timestamp = Instant.ofEpochSecond(
                event.getTimestamp().getSeconds(),
                event.getTimestamp().getNanos()
        );

        HubEventAvro.Builder builder = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(timestamp);

        switch (event.getPayloadCase()) {
            case DEVICE_ADDED -> builder.setPayload(
                    DeviceAddedEventAvro.newBuilder()
                            .setId(event.getDeviceAdded().getId())
                            .setDeviceType(DeviceTypeAvro.valueOf(
                                    event.getDeviceAdded().getType().name()
                            ))
                            .build()
            );

            case DEVICE_REMOVED -> builder.setPayload(
                    DeviceRemovedEventAvro.newBuilder()
                            .setId(event.getDeviceRemoved().getId())
                            .build()
            );

            case SCENARIO_ADDED -> builder.setPayload(
                    ScenarioAddedEventAvro.newBuilder()
                            .setName(event.getScenarioAdded().getName())
                            .setConditions(event.getScenarioAdded()
                                    .getConditionList()
                                    .stream()
                                    .map(this::toConditionAvro)
                                    .collect(Collectors.toList()))
                            .setActions(event.getScenarioAdded()
                                    .getActionList()
                                    .stream()
                                    .map(this::toActionAvro)
                                    .collect(Collectors.toList()))
                            .build()
            );

            case SCENARIO_REMOVED -> builder.setPayload(
                    ScenarioRemovedEventAvro.newBuilder()
                            .setName(event.getScenarioRemoved().getName())
                            .build()
            );

            case PAYLOAD_NOT_SET ->
                    throw new IllegalArgumentException("Hub event payload is not set");
        }

        return builder.build();
    }

    private ScenarioConditionAvro toConditionAvro(ScenarioConditionProto condition) {
        Object value = switch (condition.getValueCase()) {
            case BOOL_VALUE -> condition.getBoolValue();
            case INT_VALUE -> condition.getIntValue();
            case VALUE_NOT_SET ->
                    throw new IllegalArgumentException("Scenario condition value is not set");
        };

        ConditionTypeAvro type = switch (condition.getType()) {
            case CO2LEVEL -> ConditionTypeAvro.CO2_LEVEL;
            default -> ConditionTypeAvro.valueOf(condition.getType().name());
        };

        return ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(type)
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
                .setValue(value)
                .build();
    }

    private DeviceActionAvro toActionAvro(DeviceActionProto action) {
        DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeAvro.valueOf(action.getType().name()));

        if (action.hasValue()) {
            builder.setValue(action.getValue());
        }

        return builder.build();
    }
}

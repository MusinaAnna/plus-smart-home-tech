package ru.yandex.practicum.kafka.telemetry.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.collector.dto.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.stream.Collectors;

@Component
public class HubEventMapper {

    public HubEventAvro toAvro(HubEvent event) {
        System.out.println("=== Mapping HubEvent: " + event.getClass().getSimpleName() +
                ", hubId=" + event.getHubId() +
                ", timestamp=" + event.getTimestamp());

        HubEventAvro.Builder builder = HubEventAvro.newBuilder();
        builder.setHubId(event.getHubId());
        builder.setTimestamp(event.getTimestamp());

        if (event instanceof DeviceAddedEvent e) {
            DeviceAddedEventAvro payload = DeviceAddedEventAvro.newBuilder()
                    .setId(e.getId())
                    .setDeviceType(DeviceTypeAvro.valueOf(e.getDeviceType()))
                    .build();
            builder.setPayload(payload);
        } else if (event instanceof DeviceRemovedEvent e) {
            DeviceRemovedEventAvro payload = DeviceRemovedEventAvro.newBuilder()
                    .setId(e.getId())
                    .build();
            builder.setPayload(payload);
        } else if (event instanceof ScenarioAddedEvent e) {
            ScenarioAddedEventAvro payload = ScenarioAddedEventAvro.newBuilder()
                    .setName(e.getName())
                    .setConditions(e.getConditions().stream()
                            .map(this::toConditionAvro)
                            .collect(Collectors.toList()))
                    .setActions(e.getActions().stream()
                            .map(this::toActionAvro)
                            .collect(Collectors.toList()))
                    .build();
            builder.setPayload(payload);
        } else if (event instanceof ScenarioRemovedEvent e) {
            ScenarioRemovedEventAvro payload = ScenarioRemovedEventAvro.newBuilder()
                    .setName(e.getName())
                    .build();
            builder.setPayload(payload);
        } else {
            throw new IllegalArgumentException("Unknown hub event type: " + event.getClass());
        }

        HubEventAvro avro = builder.build();
        System.out.println("=== Built HubEventAvro: " + avro);
        return avro;
    }

    private ScenarioConditionAvro toConditionAvro(ScenarioCondition condition) {
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType()))
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation()))
                .setValue(condition.getValue())
                .build();
    }

    private DeviceActionAvro toActionAvro(DeviceAction action) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeAvro.valueOf(action.getType()))
                .setValue(action.getValue())
                .build();
    }
}

package ru.yandex.practicum.kafka.telemetry.collector.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class DeviceAddedEvent extends HubEvent {
    private String id;
    private String deviceType;

    @Override
    public String getType() {
        return "DEVICE_ADDED";
    }
}

package ru.yandex.practicum.kafka.telemetry.collector.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class DeviceRemovedEvent extends HubEvent {
    private String id;

    @Override
    public String getType() {
        return "DEVICE_REMOVED";
    }
}

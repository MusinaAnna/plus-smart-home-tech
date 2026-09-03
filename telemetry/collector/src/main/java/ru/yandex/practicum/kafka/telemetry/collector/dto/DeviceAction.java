package ru.yandex.practicum.kafka.telemetry.collector.dto;

import lombok.Data;

@Data
public class DeviceAction {
    private String sensorId;
    private String type;
    private Integer value;
}

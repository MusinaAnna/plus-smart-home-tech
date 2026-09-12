package ru.yandex.practicum.kafka.telemetry.collector.dto;

import lombok.Data;

@Data
public class ScenarioCondition {
    private String sensorId;
    private String type;
    private String operation;
    private Integer value;
}

package ru.yandex.practicum.kafka.telemetry.collector.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ClimateSensorEvent extends SensorEvent {
    private int temperatureC;
    private int humidity;
    private int co2Level;

    @Override
    public String getType() {
        return "CLIMATE_SENSOR_EVENT";
    }
}

package ru.yandex.practicum.kafka.telemetry.collector.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class LightSensorEvent extends SensorEvent {
    private int linkQuality;
    private int luminosity;

    @Override
    public String getType() {
        return "LIGHT_SENSOR_EVENT";
    }
}

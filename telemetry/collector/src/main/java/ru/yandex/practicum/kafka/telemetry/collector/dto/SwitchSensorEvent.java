package ru.yandex.practicum.kafka.telemetry.collector.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class SwitchSensorEvent extends SensorEvent {
    private boolean state;     // поле уже есть

    @Override
    public String getType() {
        return "SWITCH_SENSOR_EVENT";
    }
}

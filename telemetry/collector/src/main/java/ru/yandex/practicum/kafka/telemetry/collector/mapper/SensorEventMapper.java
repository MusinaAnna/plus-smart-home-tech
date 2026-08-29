package ru.yandex.practicum.kafka.telemetry.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.collector.dto.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

@Component
public class SensorEventMapper {

    public SensorEventAvro toAvro(SensorEvent event) {
        System.out.println("=== Mapping SensorEvent: " + event.getClass().getSimpleName() +
                ", id=" + event.getId() +
                ", hubId=" + event.getHubId() +
                ", timestamp=" + event.getTimestamp());

        SensorEventAvro.Builder builder = SensorEventAvro.newBuilder();
        builder.setId(event.getId());
        builder.setHubId(event.getHubId());
        builder.setTimestamp(event.getTimestamp());

        if (event instanceof ClimateSensorEvent e) {
            builder.setPayload(ClimateSensorAvro.newBuilder()
                    .setTemperatureC(e.getTemperatureC())
                    .setHumidity(e.getHumidity())
                    .setCo2Level(e.getCo2Level())
                    .build());
        } else if (event instanceof LightSensorEvent e) {
            builder.setPayload(LightSensorAvro.newBuilder()
                    .setLinkQuality(e.getLinkQuality())
                    .setLuminosity(e.getLuminosity())
                    .build());
        } else if (event instanceof MotionSensorEvent e) {
            builder.setPayload(MotionSensorAvro.newBuilder()
                    .setLinkQuality(e.getLinkQuality())
                    .setMotion(e.isMotion())
                    .setVoltage(e.getVoltage())
                    .build());
        } else if (event instanceof SwitchSensorEvent e) {
            builder.setPayload(SwitchSensorAvro.newBuilder()
                    .setState(e.isState())
                    .build());
        } else if (event instanceof TemperatureSensorEvent e) {
            builder.setPayload(TemperatureSensorAvro.newBuilder()
                    .setTemperatureC(e.getTemperatureC())
                    .setTemperatureF(e.getTemperatureF())
                    .build());
        } else {
            throw new IllegalArgumentException("Unknown sensor event type: " + event.getClass());
        }

        SensorEventAvro avro = builder.build();
        System.out.println("=== Built SensorEventAvro: " + avro);
        return avro;
    }
}

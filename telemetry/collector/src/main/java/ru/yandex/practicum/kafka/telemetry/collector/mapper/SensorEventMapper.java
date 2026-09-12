package ru.yandex.practicum.kafka.telemetry.collector.mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.collector.dto.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

@Component
public class SensorEventMapper {

    private static final Logger log = LoggerFactory.getLogger(SensorEventMapper.class);

    public SensorEventAvro toAvro(SensorEvent event) {
        log.debug("Mapping SensorEvent: {}", event.getClass().getSimpleName());

        Object payload = buildPayload(event);

        SensorEventAvro avro = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();

        log.debug("Built SensorEventAvro: {}", avro);
        return avro;
    }

    private Object buildPayload(SensorEvent event) {
        if (event instanceof ClimateSensorEvent e) {
            return ClimateSensorAvro.newBuilder()
                    .setTemperatureC(e.getTemperatureC())
                    .setHumidity(e.getHumidity())
                    .setCo2Level(e.getCo2Level())
                    .build();
        } else if (event instanceof LightSensorEvent e) {
            return LightSensorAvro.newBuilder()
                    .setLinkQuality(e.getLinkQuality())
                    .setLuminosity(e.getLuminosity())
                    .build();
        } else if (event instanceof MotionSensorEvent e) {
            return MotionSensorAvro.newBuilder()
                    .setLinkQuality(e.getLinkQuality())
                    .setMotion(e.isMotion())
                    .setVoltage(e.getVoltage())
                    .build();
        } else if (event instanceof SwitchSensorEvent e) {
            return SwitchSensorAvro.newBuilder()
                    .setState(e.isState())   // ← без инвертирования!
                    .build();
        } else if (event instanceof TemperatureSensorEvent e) {
            return TemperatureSensorAvro.newBuilder()
                    .setTemperatureC(e.getTemperatureC())
                    .setTemperatureF(e.getTemperatureF())
                    .build();
        } else {
            throw new IllegalArgumentException("Unknown sensor event type: " + event.getClass());
        }
    }
}

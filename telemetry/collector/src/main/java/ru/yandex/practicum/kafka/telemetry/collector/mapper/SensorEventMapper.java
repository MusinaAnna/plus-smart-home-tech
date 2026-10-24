package ru.yandex.practicum.kafka.telemetry.collector.mapper;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;

@Slf4j
@Component
public class SensorEventMapper {


    public SensorEventAvro toAvro(SensorEventProto event) {
        log.debug("Mapping SensorEventProto: {}", event.getPayloadCase());

        Object payload = buildPayload(event);

        Instant timestamp = Instant.ofEpochSecond(
                event.getTimestamp().getSeconds(),
                event.getTimestamp().getNanos()
        );

        SensorEventAvro avro = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(timestamp)
                .setPayload(payload)
                .build();

        log.debug("Built SensorEventAvro: {}", avro);
        return avro;
    }

    private Object buildPayload(SensorEventProto event) {
        return switch (event.getPayloadCase()) {
            case CLIMATE_SENSOR -> ClimateSensorAvro.newBuilder()
                    .setTemperatureC(event.getClimateSensor().getTemperatureC())
                    .setHumidity(event.getClimateSensor().getHumidity())
                    .setCo2Level(event.getClimateSensor().getCo2Level())
                    .build();

            case LIGHT_SENSOR -> LightSensorAvro.newBuilder()
                    .setLinkQuality(event.getLightSensor().getLinkQuality())
                    .setLuminosity(event.getLightSensor().getLuminosity())
                    .build();

            case MOTION_SENSOR -> MotionSensorAvro.newBuilder()
                    .setLinkQuality(event.getMotionSensor().getLinkQuality())
                    .setMotion(event.getMotionSensor().getMotion())
                    .setVoltage(event.getMotionSensor().getVoltage())
                    .build();

            case SWITCH_SENSOR -> SwitchSensorAvro.newBuilder()
                    .setState(event.getSwitchSensor().getState())
                    .build();

            case TEMPERATURE_SENSOR -> TemperatureSensorAvro.newBuilder()
                    .setTemperatureC(event.getTemperatureSensor().getTemperatureC())
                    .setTemperatureF(event.getTemperatureSensor().getTemperatureF())
                    .build();

            case PAYLOAD_NOT_SET ->
                    throw new IllegalArgumentException("Sensor event payload is not set");
        };
    }
}

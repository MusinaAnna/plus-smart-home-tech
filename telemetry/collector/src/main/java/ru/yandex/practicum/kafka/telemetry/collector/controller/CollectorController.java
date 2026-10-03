package ru.yandex.practicum.kafka.telemetry.collector.controller;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.collector.mapper.HubEventMapper;
import ru.yandex.practicum.kafka.telemetry.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.kafka.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.util.AvroSerializer;

@Slf4j
@GrpcService
public class CollectorController
        extends CollectorControllerGrpc.CollectorControllerImplBase {

    private final SensorEventMapper sensorEventMapper;
    private final HubEventMapper hubEventMapper;
    private final AvroSerializer avroSerializer;
    private final KafkaEventProducer producer;

    public CollectorController(SensorEventMapper sensorEventMapper,
                               HubEventMapper hubEventMapper,
                               AvroSerializer avroSerializer,
                               KafkaEventProducer producer) {
        this.sensorEventMapper = sensorEventMapper;
        this.hubEventMapper = hubEventMapper;
        this.avroSerializer = avroSerializer;
        this.producer = producer;
    }

    @Override
    public void collectSensorEvent(SensorEventProto request,
                                   StreamObserver<Empty> responseObserver) {
        try {
            log.info("Received sensor event: id={}, hubId={}, type={}",
                    request.getId(),
                    request.getHubId(),
                    request.getPayloadCase());

            SensorEventAvro avroEvent = sensorEventMapper.toAvro(request);
            byte[] avroBytes = avroSerializer.serialize(avroEvent);

            producer.sendSensorEvent(
                    avroEvent,
                    request.getHubId(),
                    avroBytes
            );

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Failed to process sensor event", e);

            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getMessage())
                            .withCause(e)
            ));
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request,
                                StreamObserver<Empty> responseObserver) {
        try {
            log.info("Received hub event: hubId={}, type={}",
                    request.getHubId(),
                    request.getPayloadCase());

            HubEventAvro avroEvent = hubEventMapper.toAvro(request);
            byte[] avroBytes = avroSerializer.serialize(avroEvent);

            producer.sendHubEvent(
                    avroEvent,
                    request.getHubId(),
                    avroBytes
            );

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Failed to process hub event", e);

            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getMessage())
                            .withCause(e)
            ));
        }
    }
}

package ru.yandex.practicum.kafka.telemetry.collector.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.util.AvroSerializer;

@Configuration
public class AvroConfig {

    @Bean
    public AvroSerializer avroSerializer() {
        return new AvroSerializer();
    }
}

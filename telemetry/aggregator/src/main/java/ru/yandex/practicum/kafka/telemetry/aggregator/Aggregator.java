package ru.yandex.practicum.kafka.telemetry.aggregator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.kafka.telemetry.aggregator.service.AggregationStarter;

@SpringBootApplication
@ConfigurationPropertiesScan
public class Aggregator {

    public static void main(String[] args) {
        ConfigurableApplicationContext context =
                SpringApplication.run(Aggregator.class, args);

        AggregationStarter aggregationStarter =
                context.getBean(AggregationStarter.class);

        aggregationStarter.start();
    }
}

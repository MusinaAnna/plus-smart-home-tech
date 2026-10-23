package ru.yandex.practicum.order;

import feign.RequestInterceptor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableFeignClients
public class OrderServiceApp {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApp.class, args);
    }

    @Bean
    public RequestInterceptor sourceServiceHeaderInterceptor() {
        return template ->
                template.header("X-Source-Service", "order-service");
    }
}

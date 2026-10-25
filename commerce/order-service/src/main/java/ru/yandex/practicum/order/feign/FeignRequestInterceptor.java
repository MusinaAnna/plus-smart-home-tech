package ru.yandex.practicum.order.feign;

import feign.RequestInterceptor;
import feign.RequestTemplate;
import org.springframework.stereotype.Component;

@Component
public class FeignRequestInterceptor implements RequestInterceptor {

    private static final String SOURCE_SERVICE_HEADER = "X-Source-Service";
    private static final String SOURCE_SERVICE_NAME = "order-service";

    @Override
    public void apply(RequestTemplate template) {
        template.header(SOURCE_SERVICE_HEADER, SOURCE_SERVICE_NAME);
    }
}

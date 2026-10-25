package ru.yandex.practicum.order.feign;

import feign.RequestTemplate;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FeignRequestInterceptorTest {

    @Test
    void shouldAddSourceServiceHeader() {
        RequestTemplate template = new RequestTemplate();

        new FeignRequestInterceptor().apply(template);

        assertThat(template.headers())
                .containsKey("X-Source-Service");
        assertThat(template.headers().get("X-Source-Service"))
                .containsExactly("order-service");
    }
}

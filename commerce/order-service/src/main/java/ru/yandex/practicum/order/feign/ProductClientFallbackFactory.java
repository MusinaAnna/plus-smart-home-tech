package ru.yandex.practicum.order.feign;

import feign.FeignException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.order.exception.ExternalServiceUnavailableException;
import ru.yandex.practicum.order.exception.OrderProcessingException;

@Component
@Slf4j
public class ProductClientFallbackFactory implements FallbackFactory<ProductClient> {

    @Override
    public ProductClient create(Throwable cause) {
        return productId -> {
            if (containsStatus(cause, 404)) {
                throw new OrderProcessingException(
                        "Товар id=" + productId + " не найден"
                );
            }

            log.warn(
                    "product-service недоступен при запросе товара id={}",
                    productId,
                    cause
            );

            throw new ExternalServiceUnavailableException(
                    "Каталог временно недоступен",
                    cause
            );
        };
    }

    private boolean containsStatus(Throwable cause, int status) {
        for (Throwable current = cause;
             current != null;
             current = current.getCause()) {

            if (current instanceof FeignException exception
                    && exception.status() == status) {
                return true;
            }
        }

        return false;
    }
}

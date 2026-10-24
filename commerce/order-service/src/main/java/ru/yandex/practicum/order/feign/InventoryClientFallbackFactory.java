package ru.yandex.practicum.order.feign;

import feign.FeignException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.order.exception.ExternalServiceUnavailableException;
import ru.yandex.practicum.order.exception.OrderProcessingException;

@Component
@Slf4j
public class InventoryClientFallbackFactory implements FallbackFactory<InventoryClient> {

    @Override
    public InventoryClient create(Throwable cause) {
        return new InventoryClient() {

            @Override
            public ReserveResponse reserveStock(ReserveRequest request) {
                if (containsStatus(cause, 404)) {
                    throw new OrderProcessingException(
                            "Складская запись для товара id="
                                    + request.productId()
                                    + " не найдена"
                    );
                }

                if (containsStatus(cause, 409)) {
                    throw new OrderProcessingException(
                            "Недостаточно товара id="
                                    + request.productId()
                                    + " на складе"
                    );
                }

                log.warn(
                        "inventory-service недоступен при резервировании товара id={}",
                        request.productId(),
                        cause
                );

                throw new ExternalServiceUnavailableException(
                        "Склад временно недоступен",
                        cause
                );
            }

            @Override
            public ReserveResponse releaseStock(ReserveRequest request) {
                log.warn(
                        "inventory-service недоступен при снятии резерва товара id={}",
                        request.productId(),
                        cause
                );

                throw new ExternalServiceUnavailableException(
                        "Склад недоступен при снятии резерва",
                        cause
                );
            }
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

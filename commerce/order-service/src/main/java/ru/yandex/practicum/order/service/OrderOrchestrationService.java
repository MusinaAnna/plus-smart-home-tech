package ru.yandex.practicum.order.service;

import feign.FeignException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.dto.OrderItemRequest;
import ru.yandex.practicum.order.entity.OrderStatus;
import ru.yandex.practicum.order.exception.ExternalServiceUnavailableException;
import ru.yandex.practicum.order.exception.OrderProcessingException;
import ru.yandex.practicum.order.feign.InventoryClient;
import ru.yandex.practicum.order.feign.ProductClient;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderOrchestrationService {

    private final ProductClient productClient;
    private final InventoryClient inventoryClient;
    private final OrderService orderService;

    public OrderDto createOrder(CreateOrderRequest request) {
        Map<Long, Integer> quantities = groupQuantities(request.items());
        Map<Long, ProductClient.ProductDto> products = new LinkedHashMap<>();
        List<InventoryClient.ReserveRequest> completed = new ArrayList<>();

        try {
            for (Long productId : quantities.keySet()) {
                products.put(productId, loadProduct(productId));
            }

            List<OrderService.PreparedItem> prepared =
                    prepareItems(request.items(), products);

            for (Map.Entry<Long, Integer> entry : quantities.entrySet()) {
                InventoryClient.ReserveRequest reserveRequest =
                        new InventoryClient.ReserveRequest(
                                entry.getKey(),
                                entry.getValue()
                        );

                reserve(reserveRequest);
                completed.add(reserveRequest);
            }

            return orderService.saveOrder(
                    request,
                    prepared,
                    OrderStatus.CONFIRMED,
                    null
            );

        } catch (ExternalServiceUnavailableException failure) {
            releaseCompleted(completed, failure);

            log.warn(
                    "Заказ переведён в ожидание подтверждения: {}",
                    failure.getMessage()
            );

            return orderService.saveOrder(
                    request,
                    pendingItems(request.items(), products),
                    OrderStatus.PENDING_CONFIRMATION,
                    "Требуется ручная проверка: " + failure.getMessage()
            );

        } catch (RuntimeException failure) {
            releaseCompleted(completed, failure);
            throw failure;
        }
    }

    private Map<Long, Integer> groupQuantities(
            List<OrderItemRequest> items
    ) {
        Map<Long, Integer> quantities = new LinkedHashMap<>();

        for (OrderItemRequest item : items) {
            try {
                quantities.merge(
                        item.productId(),
                        item.quantity(),
                        Math::addExact
                );
            } catch (ArithmeticException exception) {
                throw new OrderProcessingException(
                        "Слишком большое количество товара id="
                                + item.productId()
                );
            }
        }

        return quantities;
    }

    private ProductClient.ProductDto loadProduct(Long productId) {
        ProductClient.ProductDto product;

        try {
            product = productClient.getProductById(productId);

        } catch (FeignException.NotFound exception) {
            throw new OrderProcessingException(
                    "Товар id=" + productId + " не найден"
            );

        } catch (FeignException exception) {
            throw new ExternalServiceUnavailableException(
                    "Каталог временно недоступен",
                    exception
            );
        }

        if (product == null || !productId.equals(product.id())) {
            throw new ExternalServiceUnavailableException(
                    "Каталог вернул некорректные данные товара id="
                            + productId
            );
        }

        if (!Boolean.TRUE.equals(product.active())) {
            throw new OrderProcessingException(
                    "Товар id=" + productId + " снят с продажи"
            );
        }

        if (product.name() == null
                || product.name().isBlank()
                || product.price() == null
                || product.price().compareTo(BigDecimal.ZERO) <= 0) {
            throw new OrderProcessingException(
                    "Некорректные данные товара id=" + productId
            );
        }

        return product;
    }

    private List<OrderService.PreparedItem> prepareItems(
            List<OrderItemRequest> items,
            Map<Long, ProductClient.ProductDto> products
    ) {
        List<OrderService.PreparedItem> prepared = new ArrayList<>();

        for (OrderItemRequest item : items) {
            ProductClient.ProductDto product =
                    products.get(item.productId());

            prepared.add(new OrderService.PreparedItem(
                    item.productId(),
                    product.name(),
                    item.quantity(),
                    product.price()
            ));
        }

        return prepared;
    }

    private List<OrderService.PreparedItem> pendingItems(
            List<OrderItemRequest> items,
            Map<Long, ProductClient.ProductDto> products
    ) {
        List<OrderService.PreparedItem> pending = new ArrayList<>();

        for (OrderItemRequest item : items) {
            ProductClient.ProductDto known =
                    products.get(item.productId());

            String name = known == null
                    ? "Товар #" + item.productId() + " (ожидает проверки)"
                    : known.name();

            BigDecimal price = known == null
                    ? BigDecimal.ZERO
                    : known.price();

            pending.add(new OrderService.PreparedItem(
                    item.productId(),
                    name,
                    item.quantity(),
                    price
            ));
        }

        return pending;
    }

    private void reserve(InventoryClient.ReserveRequest request) {
        InventoryClient.ReserveResponse response;

        try {
            response = inventoryClient.reserveStock(request);

        } catch (FeignException.NotFound exception) {
            throw new OrderProcessingException(
                    "Складская запись для товара id="
                            + request.productId()
                            + " не найдена"
            );

        } catch (FeignException.Conflict exception) {
            throw new OrderProcessingException(
                    "Недостаточно товара id="
                            + request.productId()
                            + " на складе"
            );

        } catch (FeignException exception) {
            throw new ExternalServiceUnavailableException(
                    "Склад временно недоступен",
                    exception
            );
        }

        if (response == null) {
            throw new ExternalServiceUnavailableException(
                    "Склад не подтвердил резервирование"
            );
        }

        if (!response.success()) {
            throw new OrderProcessingException(
                    "Недостаточно товара id="
                            + request.productId()
                            + " на складе"
            );
        }
    }

    private void releaseCompleted(
            List<InventoryClient.ReserveRequest> completed,
            RuntimeException originalFailure
    ) {
        for (int i = completed.size() - 1; i >= 0; i--) {
            InventoryClient.ReserveRequest request = completed.get(i);

            try {
                InventoryClient.ReserveResponse response =
                        inventoryClient.releaseStock(request);

                if (response == null || !response.success()) {
                    IllegalStateException failure =
                            new IllegalStateException(
                                    "Склад не подтвердил снятие резерва товара id="
                                            + request.productId()
                            );

                    originalFailure.addSuppressed(failure);

                    log.error(
                            "Не подтверждено снятие резерва: productId={}",
                            request.productId()
                    );
                }

            } catch (RuntimeException releaseFailure) {
                originalFailure.addSuppressed(releaseFailure);

                log.error(
                        "Не удалось снять резерв: productId={}",
                        request.productId(),
                        releaseFailure
                );
            }
        }
    }
}

package ru.yandex.practicum.order.service;

import feign.FeignException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.dto.OrderItemRequest;
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

        Map<Long, ProductClient.ProductDto> products =
                new LinkedHashMap<>();

        for (Long productId : quantities.keySet()) {
            products.put(productId, loadProduct(productId));
        }

        List<OrderService.PreparedItem> items = new ArrayList<>();

        for (OrderItemRequest item : request.items()) {
            ProductClient.ProductDto product =
                    products.get(item.productId());

            items.add(new OrderService.PreparedItem(
                    item.productId(),
                    product.name(),
                    item.quantity(),
                    product.price()
            ));
        }

        List<InventoryClient.ReserveRequest> completed =
                new ArrayList<>();

        try {
            for (Map.Entry<Long, Integer> entry
                    : quantities.entrySet()) {

                InventoryClient.ReserveRequest reserveRequest =
                        new InventoryClient.ReserveRequest(
                                entry.getKey(),
                                entry.getValue()
                        );

                reserve(reserveRequest);
                completed.add(reserveRequest);
            }

            return orderService.saveOrder(request, items);

        } catch (RuntimeException exception) {
            releaseCompleted(completed, exception);
            throw exception;
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
            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "Сервис каталога временно недоступен",
                    exception
            );
        }

        if (product == null
                || !productId.equals(product.id())) {
            throw new OrderProcessingException(
                    "Не удалось получить корректные данные товара id="
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

    private void reserve(
            InventoryClient.ReserveRequest request
    ) {
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
            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "Сервис склада временно недоступен",
                    exception
            );
        }

        if (response == null) {
            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "Не получен ответ от сервиса склада"
            );
        }

        if (!response.success()) {
            throw new OrderProcessingException(
                    "Не удалось зарезервировать товар id="
                            + request.productId()
                            + ": недостаточно товара на складе"
            );
        }
    }

    private void releaseCompleted(
            List<InventoryClient.ReserveRequest> completed,
            RuntimeException originalException
    ) {
        for (int i = completed.size() - 1; i >= 0; i--) {
            InventoryClient.ReserveRequest request = completed.get(i);

            try {
                InventoryClient.ReserveResponse response =
                        inventoryClient.releaseStock(request);

                if (response == null || !response.success()) {
                    IllegalStateException releaseFailure =
                            new IllegalStateException(
                                    "Склад не подтвердил снятие резерва"
                            );

                    originalException.addSuppressed(releaseFailure);

                    log.error(
                            "Снятие резерва не подтверждено: productId={}",
                            request.productId()
                    );
                }

            } catch (RuntimeException releaseException) {
                originalException.addSuppressed(releaseException);

                log.error(
                        "Ошибка снятия резерва: productId={}",
                        request.productId(),
                        releaseException
                );
            }
        }
    }
}

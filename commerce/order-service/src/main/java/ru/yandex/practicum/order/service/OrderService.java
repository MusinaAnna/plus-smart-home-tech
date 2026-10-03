package ru.yandex.practicum.order.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.dto.OrderItemDto;
import ru.yandex.practicum.order.dto.OrderItemRequest;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.entity.OrderItem;
import ru.yandex.practicum.order.exception.NotFoundException;
import ru.yandex.practicum.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class OrderService {

    private final OrderRepository orderRepository;

    @Transactional
    public OrderDto create(CreateOrderRequest request) {
        Order order = new Order();
        order.setCustomerName(request.customerName());
        order.setCustomerEmail(request.customerEmail());
        order.setStatus("CREATED");
        order.setCreatedAt(LocalDateTime.now());

        BigDecimal totalPrice = BigDecimal.ZERO;

        for (OrderItemRequest itemRequest : request.items()) {
            OrderItem item = new OrderItem();
            item.setProductId(itemRequest.productId());
            item.setProductName(itemRequest.productName());
            item.setQuantity(itemRequest.quantity());
            item.setPrice(itemRequest.price());
            item.setOrder(order);

            order.getItems().add(item);

            totalPrice = totalPrice.add(
                    itemRequest.price().multiply(
                            BigDecimal.valueOf(itemRequest.quantity())
                    )
            );
        }

        order.setTotalPrice(totalPrice);

        return toDto(orderRepository.save(order));
    }

    public OrderDto findById(Long id) {
        Order order = orderRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Заказ не найден"));

        return toDto(order);
    }

    public List<OrderDto> findAll() {
        return orderRepository.findAll()
                .stream()
                .map(this::toDto)
                .toList();
    }

    public List<OrderDto> findByEmail(String email) {
        return orderRepository.findAllByCustomerEmail(email)
                .stream()
                .map(this::toDto)
                .toList();
    }

    private OrderDto toDto(Order order) {
        List<OrderItemDto> items = order.getItems()
                .stream()
                .map(item -> new OrderItemDto(
                        item.getId(),
                        item.getProductId(),
                        item.getProductName(),
                        item.getQuantity(),
                        item.getPrice()
                ))
                .toList();

        return new OrderDto(
                order.getId(),
                order.getCustomerName(),
                order.getCustomerEmail(),
                order.getStatus(),
                order.getTotalPrice(),
                order.getStatusDetails(),
                order.getCreatedAt(),
                items
        );
    }
}

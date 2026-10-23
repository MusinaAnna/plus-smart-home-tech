package ru.yandex.practicum.order;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderItemRequest;
import ru.yandex.practicum.order.feign.InventoryClient;
import ru.yandex.practicum.order.feign.ProductClient;
import ru.yandex.practicum.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@SpringBootTest
@AutoConfigureMockMvc
@SuppressWarnings("unchecked")
class OrderServiceAcceptanceTest {

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper json;

    @Autowired
    private OrderRepository orderRepository;

    @MockBean
    private ProductClient productClient;

    @MockBean
    private InventoryClient inventoryClient;

    @Test
    void shouldCreateConfirmedOrderWithProductSnapshots() throws Exception {
        when(productClient.getProductById(1L)).thenReturn(
                product(1L, "Acceptance Smart Lamp", "3490.00", true)
        );
        when(productClient.getProductById(2L)).thenReturn(
                product(2L, "Acceptance Smart Plug", "1290.00", true)
        );

        when(inventoryClient.reserveStock(
                new InventoryClient.ReserveRequest(1L, 2)
        )).thenReturn(success());

        when(inventoryClient.reserveStock(
                new InventoryClient.ReserveRequest(2L, 1)
        )).thenReturn(success());

        CreateOrderRequest request = new CreateOrderRequest(
                "Acceptance Buyer",
                "acceptance-buyer@example.com",
                List.of(
                        new OrderItemRequest(1L, 2),
                        new OrderItemRequest(2L, 1)
                )
        );

        MvcResult response = postJson("/api/orders", request);

        assertThat(response.getResponse().getStatus()).isEqualTo(201);

        Map<String, Object> created = readMap(response);

        assertThat(created.get("status")).isEqualTo("CONFIRMED");
        assertThat(asDecimal(created.get("totalPrice")))
                .isEqualByComparingTo("8270.00");

        List<Map<String, Object>> items =
                (List<Map<String, Object>>) created.get("items");

        assertThat(items).hasSize(2);
        assertThat(items)
                .anySatisfy(item -> {
                    assertThat(item.get("productName"))
                            .isEqualTo("Acceptance Smart Lamp");
                    assertThat(asDecimal(item.get("price")))
                            .isEqualByComparingTo("3490.00");
                });

        Long orderId = ((Number) created.get("id")).longValue();

        assertThat(
                mvc.perform(get("/api/orders/{id}", orderId))
                        .andReturn()
                        .getResponse()
                        .getStatus()
        ).isEqualTo(200);

        MvcResult byEmail = mvc.perform(
                get("/api/orders/by-email")
                        .param("email", "acceptance-buyer@example.com")
        ).andReturn();

        assertThat(byEmail.getResponse().getStatus()).isEqualTo(200);

        List<Map<String, Object>> orders = json.readValue(
                byEmail.getResponse().getContentAsString(),
                new TypeReference<>() {
                }
        );

        assertThat(orders)
                .anySatisfy(order ->
                        assertThat(order.get("id"))
                                .isEqualTo(created.get("id"))
                );
    }

    @Test
    void shouldReserveRepeatedProductOnlyOnce() throws Exception {
        when(productClient.getProductById(7L)).thenReturn(
                product(7L, "Smart Sensor", "100.00", true)
        );

        when(inventoryClient.reserveStock(
                new InventoryClient.ReserveRequest(7L, 3)
        )).thenReturn(success());

        CreateOrderRequest request = new CreateOrderRequest(
                "Buyer",
                "buyer@example.com",
                List.of(
                        new OrderItemRequest(7L, 2),
                        new OrderItemRequest(7L, 1)
                )
        );

        MvcResult response = postJson("/api/orders", request);

        assertThat(response.getResponse().getStatus()).isEqualTo(201);
        assertThat(asDecimal(readMap(response).get("totalPrice")))
                .isEqualByComparingTo("300.00");

        verify(productClient).getProductById(7L);
        verify(inventoryClient).reserveStock(
                new InventoryClient.ReserveRequest(7L, 3)
        );
    }

    @Test
    void shouldRejectInactiveProduct() throws Exception {
        when(productClient.getProductById(20L)).thenReturn(
                product(20L, "Inactive Product", "100.00", false)
        );

        long before = orderRepository.count();

        MvcResult response = postJson(
                "/api/orders",
                new CreateOrderRequest(
                        "Buyer",
                        "buyer@example.com",
                        List.of(new OrderItemRequest(20L, 1))
                )
        );

        assertThat(response.getResponse().getStatus()).isEqualTo(422);
        assertThat(orderRepository.count()).isEqualTo(before);

        verifyNoInteractions(inventoryClient);
    }

    @Test
    void shouldReleaseFirstReservationWhenSecondFails()
            throws Exception {
        when(productClient.getProductById(41L)).thenReturn(
                product(41L, "First Product", "100.00", true)
        );
        when(productClient.getProductById(42L)).thenReturn(
                product(42L, "Second Product", "200.00", true)
        );

        InventoryClient.ReserveRequest first =
                new InventoryClient.ReserveRequest(41L, 2);

        InventoryClient.ReserveRequest second =
                new InventoryClient.ReserveRequest(42L, 1);

        when(inventoryClient.reserveStock(first))
                .thenReturn(success());

        when(inventoryClient.reserveStock(second))
                .thenReturn(
                        new InventoryClient.ReserveResponse(
                                false,
                                0,
                                "Недостаточно товара"
                        )
                );

        when(inventoryClient.releaseStock(first))
                .thenReturn(success());

        long before = orderRepository.count();

        MvcResult response = postJson(
                "/api/orders",
                new CreateOrderRequest(
                        "Buyer",
                        "buyer@example.com",
                        List.of(
                                new OrderItemRequest(41L, 2),
                                new OrderItemRequest(42L, 1)
                        )
                )
        );

        assertThat(response.getResponse().getStatus()).isEqualTo(422);
        assertThat(orderRepository.count()).isEqualTo(before);

        verify(inventoryClient).releaseStock(first);
    }

    @Test
    void shouldReturnBadRequestForInvalidOrderPayload()
            throws Exception {
        MvcResult response = postJson(
                "/api/orders",
                new CreateOrderRequest(
                        "",
                        "not-an-email",
                        List.of()
                )
        );

        assertThat(response.getResponse().getStatus()).isEqualTo(400);
        assertThat(readMap(response))
                .containsKeys("message", "validationErrors");
    }

    private ProductClient.ProductDto product(
            Long id,
            String name,
            String price,
            boolean active
    ) {
        return new ProductClient.ProductDto(
                id,
                name,
                null,
                new BigDecimal(price),
                active
        );
    }

    private InventoryClient.ReserveResponse success() {
        return new InventoryClient.ReserveResponse(
                true,
                10,
                "Успешно"
        );
    }

    private MvcResult postJson(String url, Object request)
            throws Exception {
        return mvc.perform(
                post(url)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(request))
        ).andReturn();
    }

    private Map<String, Object> readMap(MvcResult response)
            throws Exception {
        return json.readValue(
                response.getResponse().getContentAsString(),
                new TypeReference<>() {
                }
        );
    }

    private BigDecimal asDecimal(Object value) {
        return new BigDecimal(value.toString());
    }
}

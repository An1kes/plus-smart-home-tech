package ru.yandex.practicum.order.client;

public record ReserveClientRequest(
        Long productId,
        Integer quantity
) {
}
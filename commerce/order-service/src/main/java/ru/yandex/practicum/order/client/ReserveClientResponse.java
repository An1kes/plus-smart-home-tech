package ru.yandex.practicum.order.client;

public record ReserveClientResponse(
        boolean success,
        Integer availableQuantity,
        String message
) {
}
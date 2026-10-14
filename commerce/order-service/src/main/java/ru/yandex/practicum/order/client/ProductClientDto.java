package ru.yandex.practicum.order.client;

import java.math.BigDecimal;

public record ProductClientDto(
        Long id,
        String name,
        String description,
        BigDecimal price,
        String imageUrl,
        Boolean active
) {
}
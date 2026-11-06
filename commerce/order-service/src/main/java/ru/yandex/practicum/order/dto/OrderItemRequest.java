package ru.yandex.practicum.order.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;

public record OrderItemRequest(

        @NotNull(message = "ID товара обязателен")
        Long productId,

        String productName,

        @NotNull(message = "Количество обязательно")
        @Min(value = 1, message = "Количество должно быть не менее 1")
        Integer quantity,

        BigDecimal price
) {
    // Дополнительный конструктор для нового формата (только productId и quantity)
    public OrderItemRequest(Long productId, Integer quantity) {
        this(productId, null, quantity, null);
    }
}
package ru.yandex.practicum.order.exception;

public class ProductServiceUnavailableException extends RuntimeException {
    private final Long productId;

    public ProductServiceUnavailableException(Long productId, Throwable cause) {
        super(String.format("Каталог товаров временно недоступен при обращении к productId=%d", productId), cause);
        this.productId = productId;
    }

    public Long getProductId() {
        return productId;
    }
}
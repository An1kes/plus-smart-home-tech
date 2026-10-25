package ru.yandex.practicum.order.client;

import feign.FeignException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.order.exception.ProductServiceUnavailableException;

@Slf4j
@Component
public class ProductClientFallbackFactory implements FallbackFactory<ProductClient> {

    @Override
    public ProductClient create(Throwable cause) {
        return id -> {
            if (cause instanceof FeignException.NotFound) {
                throw (FeignException.NotFound) cause;
            }
            if (cause instanceof FeignException feignEx && feignEx.status() >= 400 && feignEx.status() < 500) {
                throw feignEx;
            }

            log.warn("Сработал Fallback для ProductClient (productId={}): {}", id, cause.getMessage());
            throw new ProductServiceUnavailableException(id, cause);
        };
    }
}
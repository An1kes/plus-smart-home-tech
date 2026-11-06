package ru.yandex.practicum.order.client;

import feign.FeignException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.order.exception.InventoryServiceUnavailableException;

@Slf4j
@Component
public class InventoryClientFallbackFactory implements FallbackFactory<InventoryClient> {

    @Override
    public InventoryClient create(Throwable cause) {
        return new InventoryClient() {
            @Override
            public ReserveClientResponse reserveProduct(ReserveClientRequest request) {
                if (cause instanceof FeignException.NotFound) {
                    throw (FeignException.NotFound) cause;
                }
                if (cause instanceof FeignException.Conflict) {
                    throw (FeignException.Conflict) cause;
                }
                if (cause instanceof FeignException feignEx && feignEx.status() >= 400 && feignEx.status() < 500) {
                    throw feignEx;
                }

                log.warn("Сработал Fallback для InventoryClient.reserve (productId={}): {}", request.productId(), cause.getMessage());
                throw new InventoryServiceUnavailableException(request.productId(), cause);
            }

            @Override
            public ReserveClientResponse releaseProduct(ReserveClientRequest request) {
                log.warn("Сработал Fallback для InventoryClient.release (productId={}): {}", request.productId(), cause.getMessage());
                throw new InventoryServiceUnavailableException(request.productId(), cause);
            }
        };
    }
}
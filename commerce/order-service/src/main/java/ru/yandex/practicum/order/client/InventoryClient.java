package ru.yandex.practicum.order.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(
        name = "inventory-service",
        fallbackFactory = InventoryClientFallbackFactory.class
)
public interface InventoryClient {

    @PostMapping("/api/inventory/reserve")
    ReserveClientResponse reserveProduct(@RequestBody ReserveClientRequest request);

    @PostMapping("/api/inventory/release")
    ReserveClientResponse releaseProduct(@RequestBody ReserveClientRequest request);
}
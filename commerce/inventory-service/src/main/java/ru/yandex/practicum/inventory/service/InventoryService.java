package ru.yandex.practicum.inventory.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.inventory.dto.InventoryDto;
import ru.yandex.practicum.inventory.dto.ReserveRequest;
import ru.yandex.practicum.inventory.dto.ReserveResponse;
import ru.yandex.practicum.inventory.dto.UpdateInventoryRequest;
import ru.yandex.practicum.inventory.entity.InventoryItem;
import ru.yandex.practicum.inventory.exception.InsufficientStockException;
import ru.yandex.practicum.inventory.exception.NotFoundException;
import ru.yandex.practicum.inventory.repository.InventoryRepository;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class InventoryService {

    private final InventoryRepository inventoryRepository;

    public List<InventoryDto> getAllInventory() {
        return inventoryRepository.findAll().stream()
                .map(this::toDto)
                .toList();
    }

    public InventoryDto getInventoryByProductId(Long productId) {
        return inventoryRepository.findByProductId(productId)
                .map(this::toDto)
                .orElseThrow(() -> new NotFoundException("Товар с productId=" + productId + " не найден на складе"));
    }

    @Transactional
    public InventoryDto createInventory(UpdateInventoryRequest request) {
        if (inventoryRepository.findByProductId(request.productId()).isPresent()) {
            throw new IllegalArgumentException("Складская запись для товара с productId=" + request.productId() + " уже существует");
        }

        InventoryItem item = InventoryItem.builder()
                .productId(request.productId())
                .quantity(request.quantity())
                .reservedQuantity(0)
                .build();
        return toDto(inventoryRepository.save(item));
    }

    @Transactional
    public InventoryDto updateQuantity(UpdateInventoryRequest request) {
        InventoryItem item = inventoryRepository.findByProductId(request.productId())
                .orElseThrow(() -> new NotFoundException("Товар с productId=" + request.productId() + " не найден на складе"));

        item.setQuantity(request.quantity());
        return toDto(item);
    }

    @Transactional
    public ReserveResponse reserveProduct(ReserveRequest request) {
        InventoryItem item = inventoryRepository.findByProductId(request.productId())
                .orElseThrow(() -> new NotFoundException("Товар с productId=" + request.productId() + " не найден на складе"));

        int available = item.getAvailableQuantity();
        int requested = request.quantity();

        if (available < requested) {
            throw new InsufficientStockException(
                    String.format("Недостаточно товара на складе для productId=%d. Доступно: %d, запрошено: %d",
                            request.productId(), available, requested)
            );
        }

        item.setReservedQuantity(item.getReservedQuantity() + requested);
        log.info("Зарезервировано {} единиц товара {}. Новый остаток: {}", requested, item.getProductId(), item.getAvailableQuantity());

        return new ReserveResponse(true, item.getAvailableQuantity(), "Товар успешно зарезервирован");
    }

    @Transactional
    public ReserveResponse releaseProduct(ReserveRequest request) {
        InventoryItem item = inventoryRepository.findByProductId(request.productId())
                .orElseThrow(() -> new NotFoundException("Товар с productId=" + request.productId() + " не найден на складе"));

        int currentReserved = item.getReservedQuantity() != null ? item.getReservedQuantity() : 0;
        int releaseQuantity = request.quantity();

        if (releaseQuantity > currentReserved) {
            throw new IllegalArgumentException(
                    String.format("Нельзя снять резерв: запрошено %d, но зарезервировано только %d для productId=%d",
                            releaseQuantity, currentReserved, request.productId())
            );
        }

        item.setReservedQuantity(currentReserved - releaseQuantity);
        log.info("Снят резерв в количестве {} для productId={}. Новый доступный остаток: {}",
                releaseQuantity, item.getProductId(), item.getAvailableQuantity());

        return new ReserveResponse(true, item.getAvailableQuantity(), "Резерв успешно снят");
    }

    private InventoryDto toDto(InventoryItem item) {
        return new InventoryDto(
                item.getId(),
                item.getProductId(),
                item.getQuantity(),
                item.getReservedQuantity(),
                item.getAvailableQuantity()
        );
    }
}
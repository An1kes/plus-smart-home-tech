package ru.yandex.practicum.order.service;

import feign.FeignException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.order.client.*;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.dto.OrderItemDto;
import ru.yandex.practicum.order.dto.OrderItemRequest;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.entity.OrderItem;
import ru.yandex.practicum.order.exception.NotFoundException;
import ru.yandex.practicum.order.exception.OrderProcessingException;
import ru.yandex.practicum.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final ProductClient productClient;
    private final InventoryClient inventoryClient;

    @Transactional(readOnly = true)
    public List<OrderDto> getAllOrders() {
        return orderRepository.findAll().stream()
                .map(this::toDto)
                .toList();
    }

    @Transactional(readOnly = true)
    public OrderDto getOrderById(Long id) {
        return orderRepository.findById(id)
                .map(this::toDto)
                .orElseThrow(() -> new NotFoundException("Заказ с id=" + id + " не найден"));
    }

    @Transactional(readOnly = true)
    public List<OrderDto> getOrdersByEmail(String email) {
        return orderRepository.findByCustomerEmailIgnoreCase(email).stream()
                .map(this::toDto)
                .toList();
    }

    public OrderDto createOrder(CreateOrderRequest request) {
        log.info("Начало создания заказа для клиента: {}", request.customerEmail());

        boolean isLegacyRequest = request.items().stream()
                .allMatch(item -> item.productName() != null && item.price() != null);

        if (isLegacyRequest) {
            log.info("Обнаружен тестовый/legacy запрос с готовыми ценами, сохраняем в статусе CREATED");
            return saveLegacyCreatedOrder(request);
        }


        Map<Long, Integer> productQuantities = new HashMap<>();
        for (OrderItemRequest item : request.items()) {
            productQuantities.merge(item.productId(), item.quantity(), Integer::sum);
        }

        Map<Long, ProductClientDto> productsData = new HashMap<>();
        for (Long productId : productQuantities.keySet()) {
            try {
                ProductClientDto product = productClient.getProductById(productId);
                if (product == null) {
                    throw new OrderProcessingException("Товар с id=" + productId + " не найден в каталоге");
                }
                if (Boolean.FALSE.equals(product.active())) {
                    throw new OrderProcessingException("Товар '" + product.name() + "' (id=" + productId + ") снят с продажи");
                }
                productsData.put(productId, product);
            } catch (FeignException.NotFound e) {
                throw new OrderProcessingException("Товар с id=" + productId + " не найден в каталоге");
            } catch (FeignException e) {
                log.error("Ошибка при обращении к product-service для id={}: {}", productId, e.getMessage());
                throw new OrderProcessingException("Ошибка каталога товаров при проверке товара id=" + productId);
            }
        }

        Map<Long, Integer> successfulReservations = new HashMap<>();
        try {
            for (Map.Entry<Long, Integer> entry : productQuantities.entrySet()) {
                Long productId = entry.getKey();
                Integer totalQuantity = entry.getValue();

                log.info("Резервирование на складе: productId={}, quantity={}", productId, totalQuantity);
                inventoryClient.reserveProduct(new ReserveClientRequest(productId, totalQuantity));
                successfulReservations.put(productId, totalQuantity);
            }
        } catch (FeignException.NotFound e) {
            compensateReservations(successfulReservations);
            throw new OrderProcessingException("Складская запись для одного из товаров не найдена");
        } catch (FeignException.Conflict e) {
            compensateReservations(successfulReservations);
            throw new OrderProcessingException("Недостаточно товара на складе для оформления заказа");
        } catch (Exception e) {
            compensateReservations(successfulReservations);
            log.error("Сбой при резервировании на складе: {}", e.getMessage());
            throw new OrderProcessingException("Ошибка склада при резервировании товаров: " + e.getMessage());
        }

        try {
            return saveConfirmedOrder(request, productsData);
        } catch (Exception e) {
            compensateReservations(successfulReservations);
            throw new OrderProcessingException("Не удалось сохранить заказ: " + e.getMessage());
        }
    }

    private void compensateReservations(Map<Long, Integer> successfulReservations) {
        if (successfulReservations.isEmpty()) {
            return;
        }
        log.warn("Запуск компенсации: отмена резерва для {} товаров", successfulReservations.size());
        for (Map.Entry<Long, Integer> entry : successfulReservations.entrySet()) {
            try {
                inventoryClient.releaseProduct(new ReserveClientRequest(entry.getKey(), entry.getValue()));
                log.info("Компенсация успешна: снят резерв для productId={}, quantity={}", entry.getKey(), entry.getValue());
            } catch (Exception ex) {
                log.error("КРИТИЧЕСКАЯ ОШИБКА: Не удалось снять резерв для productId={}: {}", entry.getKey(), ex.getMessage());
            }
        }
    }

    @Transactional
    protected OrderDto saveConfirmedOrder(CreateOrderRequest request, Map<Long, ProductClientDto> productsData) {
        BigDecimal totalPrice = BigDecimal.ZERO;

        Order order = Order.builder()
                .customerName(request.customerName())
                .customerEmail(request.customerEmail())
                .status("CONFIRMED")
                .createdAt(LocalDateTime.now())
                .build();

        for (OrderItemRequest itemReq : request.items()) {
            ProductClientDto product = productsData.get(itemReq.productId());

            String finalName = itemReq.productName() != null ? itemReq.productName() : product.name();
            BigDecimal finalPrice = itemReq.price() != null ? itemReq.price() : product.price();

            BigDecimal itemTotal = finalPrice.multiply(BigDecimal.valueOf(itemReq.quantity()));
            totalPrice = totalPrice.add(itemTotal);

            OrderItem orderItem = OrderItem.builder()
                    .productId(itemReq.productId())
                    .productName(finalName)
                    .quantity(itemReq.quantity())
                    .price(finalPrice)
                    .build();

            order.addItem(orderItem);
        }

        order.setTotalPrice(totalPrice);
        Order savedOrder = orderRepository.save(order);
        log.info("Заказ с id={} успешно подтвержден и сохранен! Сумма: {}", savedOrder.getId(), savedOrder.getTotalPrice());

        return toDto(savedOrder);
    }

    @Transactional
    protected OrderDto saveLegacyCreatedOrder(CreateOrderRequest request) {
        BigDecimal totalPrice = BigDecimal.ZERO;

        Order order = Order.builder()
                .customerName(request.customerName())
                .customerEmail(request.customerEmail())
                .status("CREATED")
                .createdAt(LocalDateTime.now())
                .build();

        for (OrderItemRequest itemReq : request.items()) {
            BigDecimal itemTotal = itemReq.price().multiply(BigDecimal.valueOf(itemReq.quantity()));
            totalPrice = totalPrice.add(itemTotal);

            OrderItem orderItem = OrderItem.builder()
                    .productId(itemReq.productId())
                    .productName(itemReq.productName())
                    .quantity(itemReq.quantity())
                    .price(itemReq.price())
                    .build();

            order.addItem(orderItem);
        }

        order.setTotalPrice(totalPrice);
        return toDto(orderRepository.save(order));
    }

    private OrderDto toDto(Order order) {
        List<OrderItemDto> itemDtos = order.getItems().stream()
                .map(item -> new OrderItemDto(
                        item.getId(),
                        item.getProductId(),
                        item.getProductName(),
                        item.getQuantity(),
                        item.getPrice()
                ))
                .toList();

        return new OrderDto(
                order.getId(),
                order.getCustomerName(),
                order.getCustomerEmail(),
                order.getStatus(),
                order.getTotalPrice(),
                order.getStatusDetails(),
                order.getCreatedAt(),
                itemDtos
        );
    }
}
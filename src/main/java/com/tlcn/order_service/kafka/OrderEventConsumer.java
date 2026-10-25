package com.tlcn.order_service.kafka;

import com.tlcn.order_service.dto.kafka.StockReservationResponse;
import com.tlcn.order_service.dto.kafka.StockRollbackRequest;
import com.tlcn.order_service.enums.OrderItemStatus;
import com.tlcn.order_service.enums.PayoutStatus;
import com.tlcn.order_service.enums.SubOrderStatus;
import com.tlcn.order_service.model.OrderItem;
import com.tlcn.order_service.model.Order;
import com.tlcn.order_service.model.SubOrder;
import com.tlcn.order_service.repository.OrderRepository;
import com.tlcn.order_service.service.OrderCompensationService;
import com.tlcn.order_service.service.StockReservationAwaiter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventConsumer {

    private static final String IDEMPOTENCY_KEY_PREFIX = "kafka:order:idempotency:";
    private static final long TTL_HOURS = 24;

    private final RedisTemplate<String, String> redisTemplate;

    private final OrderRepository orderRepository;
    private final OrderEventProducer orderEventProducer;
    private final OrderCompensationService orderCompensationService;
    private final StockReservationAwaiter stockReservationAwaiter;

    @KafkaListener(
        topics = "${spring.kafka.topics.stock-reservation-response:stock-reservation-response}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeStockReservationResponse(
        StockReservationResponse response,
        @Header(name = "messageId", required = false) byte[] messageIdHeader,
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset,
        Acknowledgment ack
    ) {
        String messageId = messageIdHeader != null ? new String(messageIdHeader, StandardCharsets.UTF_8) : null;

        try {
            if (messageId != null && isProcessed(messageId)) {
                log.debug("Stock reservation response already processed: messageId={}", messageId);
                ack.acknowledge();
                return;
            }

            if (response == null || response.getOrderId() == null || response.getOrderId().isBlank()) {
                log.warn("Ignore invalid StockReservationResponse: messageId={} orderId=null", messageId);
                ack.acknowledge();
                return;
            }
            if (response.getProductId() == null || response.getRequestedQuantity() == null) {
                log.warn("Ignore invalid StockReservationResponse: messageId={} orderId={} productId={} requestedQuantity={}",
                    messageId, response.getOrderId(), response.getProductId(), response.getRequestedQuantity());
                ack.acknowledge();
                return;
            }

            log.info("Received stock-reservation-response: messageId={}, orderId={}, productId={}, success={} ({}:{}@{})",
                messageId,
                response.getOrderId(),
                response.getProductId(),
                response.isSuccess(),
                topic, partition, offset
            );

            handleStockReservationResponse(response);

            if (messageId != null) {
                markAsProcessed(messageId);
            }

            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process stock-reservation-response: messageId={}, error={}", messageId, e.getMessage(), e);
            throw new RuntimeException("Failed to process stock reservation response", e);
        }
    }

    private void handleStockReservationResponse(StockReservationResponse response) {
        Order order = orderRepository.findByOrderCode(response.getOrderId()).orElse(null);
        if (order == null) {
            log.warn("Order not found for StockReservationResponse: orderId={}", response.getOrderId());
            return;
        }

        int expectedCount = expectedItemCount(order);
        if (expectedCount <= 0) {
            log.warn("Order has no items for StockReservationResponse: orderId={}", order.getOrderCode());
            return;
        }

        String orderCode = order.getOrderCode();
        if (OrderCompensationService.isStockSagaAbandoned(order)) {
            if (response.isSuccess()) {
                sendItemRollback(orderCode, response.getProductId(), response.getRequestedQuantity());
                log.info("Rolled back late stock reservation on abandoned order: orderCode={} productId={}",
                    orderCode, response.getProductId());
            }
            stockReservationAwaiter.complete(orderCode, false);
            return;
        }

        String seenKey = OrderCompensationService.STOCK_SEEN_KEY_PREFIX + orderCode;
        String okKey = OrderCompensationService.STOCK_OK_KEY_PREFIX + orderCode;
        String itemKey = buildItemKey(response.getProductId(), response.getRequestedQuantity());

        redisTemplate.opsForSet().add(seenKey, itemKey);
        redisTemplate.expire(seenKey, TTL_HOURS, TimeUnit.HOURS);

        if (response.isSuccess()) {
            redisTemplate.opsForSet().add(okKey, itemKey);
            redisTemplate.expire(okKey, TTL_HOURS, TimeUnit.HOURS);
        }

        Long seen = redisTemplate.opsForSet().size(seenKey);
        Long ok = redisTemplate.opsForSet().size(okKey);

        log.info("Stock reservation aggregate: orderId={} expected={} seen={} ok={} lastProductId={} success={}",
            orderCode, expectedCount, seen, ok, response.getProductId(), response.isSuccess());

        if (!response.isSuccess()) {
            cancelAllSubOrders(order);
            rollbackSuccessfulReservations(orderCode, okKey);
            // Release discount + refund/cancel payment (saga compensation)
            orderCompensationService.compensateStockReservationFailure(order);
            cleanupKeys(seenKey, okKey);
            stockReservationAwaiter.complete(orderCode, false);
            return;
        }

        if (seen != null && ok != null && seen >= expectedCount && ok >= expectedCount) {
            confirmAllSubOrders(order);
            cleanupKeys(seenKey, okKey);
            stockReservationAwaiter.complete(orderCode, true);
        }
    }

    private int expectedItemCount(Order order) {
        int count = 0;
        if (order.getSubOrders() == null) {
            return 0;
        }
        for (SubOrder subOrder : order.getSubOrders()) {
            if (subOrder.getItems() != null) {
                count += subOrder.getItems().size();
            }
        }
        return count;
    }

    private String buildItemKey(Long productId, Integer requestedQuantity) {
        return productId + ":" + requestedQuantity;
    }

    private void cancelAllSubOrders(Order order) {
        if (order.getSubOrders() == null) {
            return;
        }
        for (SubOrder subOrder : order.getSubOrders()) {
            if (subOrder.getStatus() != SubOrderStatus.CANCELLED) {
                subOrder.setStatus(SubOrderStatus.CANCELLED);
            }
            subOrder.setPayoutStatus(PayoutStatus.CANCELLED);

            if (subOrder.getItems() == null) {
                continue;
            }

            for (OrderItem item : subOrder.getItems()) {
                if (item == null) {
                    continue;
                }
                if (item.getStatus() != OrderItemStatus.CANCELLED && item.getStatus() != OrderItemStatus.RETURNED) {
                    item.setStatus(OrderItemStatus.CANCELLED);
                }
            }
        }
        orderRepository.save(order);
    }

    private void confirmAllSubOrders(Order order) {
        if (order.getSubOrders() == null) {
            return;
        }
        for (SubOrder subOrder : order.getSubOrders()) {
            // Stock reserved successfully; vendor still needs to confirm each item.
            // Keep suborder status as CREATED.
        }
        orderRepository.save(order);
    }

    private void sendItemRollback(String orderCode, Long productId, Integer quantity) {
        if (orderCode == null || productId == null || quantity == null || quantity <= 0) {
            return;
        }
        StockRollbackRequest rollback = StockRollbackRequest.builder()
            .orderId(orderCode)
            .productId(productId)
            .quantity(quantity)
            .timestamp(LocalDateTime.now())
            .build();
        orderEventProducer.sendStockRollbackRequest(rollback);
    }

    private void rollbackSuccessfulReservations(String orderCode, String okKey) {
        Set<String> okItems = redisTemplate.opsForSet().members(okKey);
        if (okItems == null || okItems.isEmpty()) {
            return;
        }

        for (String itemKey : okItems) {
            ParsedItem parsed = parseItemKey(itemKey);
            if (parsed == null) {
                continue;
            }

            StockRollbackRequest rollback = StockRollbackRequest.builder()
                .orderId(orderCode)
                .productId(parsed.productId)
                .quantity(parsed.quantity)
                .timestamp(LocalDateTime.now())
                .build();
            orderEventProducer.sendStockRollbackRequest(rollback);
        }
    }

    private void cleanupKeys(String seenKey, String okKey) {
        redisTemplate.delete(seenKey);
        redisTemplate.delete(okKey);
    }

    private ParsedItem parseItemKey(String itemKey) {
        if (itemKey == null || itemKey.isBlank()) {
            return null;
        }
        String[] parts = itemKey.split(":", 2);
        if (parts.length != 2) {
            return null;
        }
        try {
            Long productId = Long.valueOf(parts[0]);
            Integer qty = Integer.valueOf(parts[1]);
            if (qty <= 0) {
                return null;
            }
            return new ParsedItem(productId, qty);
        } catch (Exception e) {
            return null;
        }
    }

    private record ParsedItem(Long productId, Integer quantity) {}

    private boolean isProcessed(String messageId) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(IDEMPOTENCY_KEY_PREFIX + messageId));
    }

    private void markAsProcessed(String messageId) {
        redisTemplate.opsForValue().set(IDEMPOTENCY_KEY_PREFIX + messageId, "processed", TTL_HOURS, TimeUnit.HOURS);
    }
}

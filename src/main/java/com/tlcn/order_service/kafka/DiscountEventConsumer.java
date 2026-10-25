package com.tlcn.order_service.kafka;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.tlcn.order_service.dto.kafka.DiscountEvent;
import com.tlcn.order_service.dto.kafka.DiscountEventType;
import com.tlcn.order_service.model.Order;
import com.tlcn.order_service.model.SubOrder;
import com.tlcn.order_service.repository.OrderRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Optional;

@Component
@RequiredArgsConstructor
@Slf4j
public class DiscountEventConsumer {

    private static final String IDEMPOTENCY_KEY_PREFIX = "kafka:discount:idempotency:";
    private static final long TTL_HOURS = 24;

    private final RedisTemplate<String, String> redisTemplate;
    private final OrderRepository orderRepository;

    @KafkaListener(
        topics = "${spring.kafka.topics.discount-events:discount.events}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeDiscountEvent(
        DiscountEvent event,
        @Header(name = "messageId", required = false) byte[] messageIdHeader,
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset,
        Acknowledgment ack
    ) {
        try {
            String messageId = messageIdHeader != null ? new String(messageIdHeader, StandardCharsets.UTF_8) : null;
            if (messageId != null && isProcessed(messageId)) {
                log.debug("Discount event already processed: messageId={}", messageId);
                ack.acknowledge();
                return;
            }

            if (event == null) {
                log.warn("Received null discount event: messageId={} ({}:{}@{})", messageId, topic, partition, offset);
                if (messageId != null) {
                    markAsProcessed(messageId);
                }
                ack.acknowledge();
                return;
            }

            log.info("Received discount event: messageId={}, type={}, orderId={}, holdId={} ({}:{}@{})",
                messageId,
                event.getType(),
                event.getOrderId(),
                event.getHoldId(),
                topic, partition, offset
            );

            // Apply saga result to Order (minimal coupling)
            applyToOrder(event);

            if (messageId != null) {
                markAsProcessed(messageId);
            }

            ack.acknowledge();
        } catch (Exception e) {
            String messageId = messageIdHeader != null ? new String(messageIdHeader, StandardCharsets.UTF_8) : null;
            log.error("Failed to process discount event: messageId={}, error={}", messageId, e.getMessage(), e);
            throw new RuntimeException("Failed to process discount event", e);
        }
    }

    private void applyToOrder(DiscountEvent event) {
        if (event.getOrderId() == null) {
            log.debug("Discount event missing orderId: type={} eventId={}", event.getType(), event.getEventId());
            return;
        }

        Optional<Order> orderOpt = orderRepository.findById(event.getOrderId());
        if (orderOpt.isEmpty()) {
            log.warn("Order not found for discount event: orderId={} type={} eventId={}",
                event.getOrderId(), event.getType(), event.getEventId());
            return;
        }

        Order order = orderOpt.get();
        DiscountEventType type = event.getType();
        if (type == null) {
            log.warn("Discount event missing type: orderId={} eventId={}", event.getOrderId(), event.getEventId());
            return;
        }

        switch (type) {
            // HOLD is now validated synchronously (request-reply) before order creation,
            // so HELD/HOLD_FAILED are not expected for persisted orders.
            case RELEASED, RELEASE_FAILED, EXPIRED -> {
                order.setPlatformDiscountHoldId(null);
                order.setPlatformDiscountAmount(BigDecimal.ZERO);
                // Keep legacy DB column in sync
                order.setDiscountAmount(BigDecimal.ZERO);
                recomputeTotalPayable(order);
                orderRepository.save(order);
            }
            case COMMITTED -> {
                // keep discount amount applied, just clear hold marker
                order.setPlatformDiscountHoldId(null);
                orderRepository.save(order);
            }
            case COMMIT_FAILED -> {
                // keep holdId for retry/manual intervention
                log.warn("Discount commit failed for orderId={} holdId={} message={}",
                    order.getId(), event.getHoldId(), event.getMessage());
            }
            case HELD, HOLD_FAILED -> {
                log.debug("Ignoring hold event for existing order: type={} orderId={} eventId={}",
                    type, event.getOrderId(), event.getEventId());
            }
        }
    }

    private static void recomputeTotalPayable(Order order) {
        BigDecimal baseTotal = Optional.ofNullable(order.getSubOrders()).orElseGet(java.util.List::of).stream()
            .map(SubOrder::getFinalAmount)
            .filter(java.util.Objects::nonNull)
            .reduce(BigDecimal.ZERO, BigDecimal::add);

        BigDecimal discount = Optional.ofNullable(order.getPlatformDiscountAmount()).orElse(BigDecimal.ZERO);
        BigDecimal totalPayable = baseTotal.subtract(discount);
        if (totalPayable.compareTo(BigDecimal.ZERO) < 0) {
            totalPayable = BigDecimal.ZERO;
        }
        order.setTotalPayableAmount(totalPayable);
    }

    private boolean isProcessed(String messageId) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(IDEMPOTENCY_KEY_PREFIX + messageId));
    }

    private void markAsProcessed(String messageId) {
        redisTemplate.opsForValue().set(IDEMPOTENCY_KEY_PREFIX + messageId, "processed", TTL_HOURS, TimeUnit.HOURS);
    }
}

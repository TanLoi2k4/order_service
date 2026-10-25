package com.tlcn.order_service.kafka;

import com.tlcn.order_service.dto.kafka.PaymentSuccess;
import com.tlcn.order_service.dto.kafka.PaymentFailed;
import com.tlcn.order_service.dto.OrderResponse;
import com.tlcn.order_service.service.CustomerOrderService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventConsumer {

    private static final String IDEMPOTENCY_KEY_PREFIX = "kafka:payment:idempotency:";

    private final CustomerOrderService customerOrderService;
    private final RedisTemplate<String, String> redisTemplate;

    @Value("${kafka.idempotency.ttl-hours:24}")
    private long idempotencyTtlHours;

    @KafkaListener(
        topics = "${spring.kafka.topics.payment-success:payment-success}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumePaymentSuccess(
        PaymentSuccess event,
        @Header(name = "messageId", required = false) String messageId,
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset,
        Acknowledgment ack
    ) {
        try {
            if (event == null || event.getOrderCode() == null || event.getOrderCode().isBlank()) {
                log.warn("Ignore invalid payment-success event: messageId={} topic={} partition={} offset={}", messageId, topic, partition, offset);
                ack.acknowledge();
                return;
            }
            if (event.getCustomerKeycloakId() == null || event.getCustomerKeycloakId().isBlank()) {
                throw new IllegalArgumentException("Payment success event missing customerKeycloakId for orderCode=" + event.getOrderCode());
            }

            try {
                customerOrderService.getMyOrderDetail(event.getCustomerKeycloakId(), event.getOrderCode());
            } catch (Exception ownershipEx) {
                log.warn(
                    "Skip payment-success because order ownership does not match: orderCode={} customerKeycloakId={} messageId={} error={}",
                    event.getOrderCode(),
                    event.getCustomerKeycloakId(),
                    messageId,
                    ownershipEx.getMessage()
                );
                ack.acknowledge();
                return;
            }

            String idempotencyKey = buildIdempotencyKey("payment-success", event.getOrderCode());
            if (isAlreadyProcessed(idempotencyKey)) {
                log.info("Skip duplicate payment-success event: orderCode={} messageId={}", event.getOrderCode(), messageId);
                ack.acknowledge();
                return;
            }

            log.info("Received payment-success: orderCode={} providerIntentId={} ({}:{}@{})", event.getOrderCode(), event.getProviderPaymentIntentId(), topic, partition, offset);

            OrderResponse updated = customerOrderService.markMyOrderPaymentCompletedInternal( event.getOrderCode());
            log.info("Order marked payment completed: orderCode={} paymentStatus={}", updated.getOrderCode(), updated.getPaymentStatus());

            markAsProcessed(idempotencyKey);

            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process payment-success: orderCode={} messageId={} error={}",
                event == null ? null : event.getOrderCode(),
                messageId,
                e.getMessage(),
                e
            );
            throw e;
        }
    }

    @KafkaListener(
        topics = "${spring.kafka.topics.payment-failed:payment-failed}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumePaymentFailed(
        PaymentFailed event,
        @Header(name = "messageId", required = false) String messageId,
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset,
        Acknowledgment ack
    ) {
        try {
            if (event == null || event.getOrderCode() == null || event.getOrderCode().isBlank()) {
                log.warn("Ignore invalid payment-failed event: messageId={} topic={} partition={} offset={}", messageId, topic, partition, offset);
                ack.acknowledge();
                return;
            }

            if (event.getCustomerKeycloakId() == null || event.getCustomerKeycloakId().isBlank()) {
                throw new IllegalArgumentException("Payment failed event missing customerKeycloakId for orderCode=" + event.getOrderCode());
            }

            try {
                customerOrderService.getMyOrderDetail(event.getCustomerKeycloakId(), event.getOrderCode());
            } catch (Exception ownershipEx) {
                log.warn(
                    "Skip payment-failed because order ownership does not match: orderCode={} customerKeycloakId={} messageId={} error={}",
                    event.getOrderCode(),
                    event.getCustomerKeycloakId(),
                    messageId,
                    ownershipEx.getMessage()
                );
                ack.acknowledge();
                return;
            }

            String idempotencyKey = buildIdempotencyKey("payment-failed", event.getOrderCode());
            if (isAlreadyProcessed(idempotencyKey)) {
                log.info("Skip duplicate payment-failed event: orderCode={} messageId={}", event.getOrderCode(), messageId);
                ack.acknowledge();
                return;
            }

            log.info("Received payment-failed: orderCode={} failureCode={} failureMessage={} ({}:{}@{})",
                event.getOrderCode(), event.getFailureCode(), event.getFailureMessage(), topic, partition, offset);

            OrderResponse updated = customerOrderService.handlePaymentFailed(
                event.getOrderCode(),
                event.getFailureCode(),
                event.getFailureMessage()
            );
            log.info("Order marked payment failed: orderCode={} paymentStatus={}", updated.getOrderCode(), updated.getPaymentStatus());

            markAsProcessed(idempotencyKey);

            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process payment-failed: orderCode={} messageId={} error={}",
                event == null ? null : event.getOrderCode(),
                messageId,
                e.getMessage(),
                e
            );
            throw e;
        }
    }

    private String buildIdempotencyKey(String eventType, String orderCode) {
        return IDEMPOTENCY_KEY_PREFIX + eventType + ":" + orderCode;
    }

    private boolean isAlreadyProcessed(String key) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(key));
    }

    private void markAsProcessed(String key) {
        redisTemplate.opsForValue().set(key, "processed", Math.max(1L, idempotencyTtlHours), TimeUnit.HOURS);
    }
}

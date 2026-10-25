package com.tlcn.order_service.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.tlcn.order_service.dto.kafka.DiscountEvent;
import com.tlcn.order_service.service.DiscountReplyAwaiter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class DiscountReplyConsumer {

    private static final String IDEMPOTENCY_KEY_PREFIX = "kafka:discount-reply:idempotency:";

    private final DiscountReplyAwaiter discountReplyAwaiter;
    private final RedisTemplate<String, String> redisTemplate;

    @Value("${kafka.idempotency.ttl-seconds:900}")
    private long idempotencyTtlSeconds;

    @KafkaListener(
        topics = "${spring.kafka.topics.discount-replies:discount.replies}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeReply(
        DiscountEvent event,
        @Header(name = "correlationId", required = false) byte[] correlationIdHeader,
        @Header(name = "messageId", required = false) byte[] messageIdHeader,
        @Header(name = "idempotency-key", required = false) byte[] idempotencyKeyHeader,
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset,
        Acknowledgment ack
    ) {
        try {
            String correlationId = decodeHeader(correlationIdHeader);
            String messageId = decodeHeader(messageIdHeader);
            String idempotencyKey = decodeHeader(idempotencyKeyHeader);

            if (idempotencyKey == null || idempotencyKey.isBlank()) {
                idempotencyKey = (messageId != null && !messageId.isBlank())
                    ? messageId
                    : correlationId + ":" + partition + ":" + offset;
            }

            if (isAlreadyProcessed(idempotencyKey)) {
                log.warn("Skip duplicate discount reply: idempotencyKey={} correlationId={} messageId={} type={} ({}:{}@{})",
                    idempotencyKey,
                    correlationId,
                    messageId,
                    event != null ? event.getType() : null,
                    topic,
                    partition,
                    offset
                );
                ack.acknowledge();
                return;
            }

            if (correlationId == null || correlationId.isBlank()) {
                log.warn("Received discount reply without correlationId: messageId={} type={} ({}:{}@{})",
                    messageId,
                    event != null ? event.getType() : null,
                    topic, partition, offset
                );
                markProcessed(idempotencyKey);
                ack.acknowledge();
                return;
            }

            boolean stored = discountReplyAwaiter.complete(correlationId, event);
            if (!stored) {
                log.warn("Failed to store discount reply in Redis: correlationId={} messageId={} type={} ({}:{}@{})",
                    correlationId,
                    messageId,
                    event != null ? event.getType() : null,
                    topic, partition, offset
                );
            } else {
                log.info("Stored discount reply for awaiter: correlationId={} messageId={} type={} ({}:{}@{})",
                    correlationId,
                    messageId,
                    event != null ? event.getType() : null,
                    topic, partition, offset
                );
            }

            markProcessed(idempotencyKey);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Failed to consume discount reply: err={}", ex.getMessage(), ex);
            throw ex;
        }
    }

    private String decodeHeader(byte[] value) {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    private boolean isAlreadyProcessed(String key) {
        return key != null && Boolean.TRUE.equals(redisTemplate.hasKey(IDEMPOTENCY_KEY_PREFIX + key));
    }

    private void markProcessed(String key) {
        if (key == null || key.isBlank()) {
            return;
        }
        redisTemplate.opsForValue().set(
            IDEMPOTENCY_KEY_PREFIX + key,
            "processed",
            Duration.ofSeconds(Math.max(1L, idempotencyTtlSeconds))
        );
    }
}

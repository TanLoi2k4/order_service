package com.tlcn.order_service.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryConsumer {

    @Qualifier("stockRedisTemplate")
    private final RedisTemplate<String, Integer> redisTemplate;

    @KafkaListener(topics = "inventory-updates", groupId = "order-service-stock-cache-updater")
    public void consumeInventoryUpdate(Map<String, Object> message, Acknowledgment acknowledgment) {
        try {
            log.info("Received inventory update for cache: {}", message);

            // Kiểm tra tin nhắn hợp lệ
            if (!validateMessage(message)) {
                acknowledgment.acknowledge();
                return;
            }

            Object source = message.get("source");
            // Bỏ qua tin nhắn từ Order Service (đảm bảo cache chỉ được cập nhật từ nguồn đáng tin cậy: Product Service)
            if (source != null && "order-service".equals(source)) {
                log.debug("Skipping message originated from order-service.");
                acknowledgment.acknowledge();
                return;
            }

            // Extract and validate types
            Long productId = ((Number) message.get("productId")).longValue();
            Integer stock = ((Number) message.get("stock")).intValue();

            if (stock < 0) {
                log.warn("Invalid stock value: productId={}, stock={}. Acknowledging.", productId, stock);
                acknowledgment.acknowledge();
                return;
            }

            // Update Redis cache 
            redisTemplate.opsForValue().set("stock:" + productId, stock);
            log.info("Updated stock cache for productId={} with stock={}", productId, stock);

            acknowledgment.acknowledge(); // Acknowledge successful processing
        } catch (IllegalArgumentException e) {
            log.error("Non-retryable error: Invalid message format. Let DefaultErrorHandler handle DLQ: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Error processing inventory update (may be transient): {}", e.getMessage(), e);
            throw e; 
        }
    }

    private boolean validateMessage(Map<String, Object> message) {
        if (message == null || !message.containsKey("productId") || !message.containsKey("stock")) {
            log.warn("Invalid message format: {}", message);
            throw new IllegalArgumentException("Missing required fields in message");
        }

        Object productIdObj = message.get("productId");
        Object stockObj = message.get("stock");
        if (!(productIdObj instanceof Number) || !(stockObj instanceof Number)) {
            log.warn("Invalid types in message: productId={}, stock={}", productIdObj, stockObj);
            throw new IllegalArgumentException("Invalid types for productId or stock");
        }

        return true;
    }
}
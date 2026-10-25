package com.tlcn.order_service.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tlcn.order_service.dto.kafka.DiscountEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Cluster-safe discount request-reply awaiter backed by Redis (LPUSH / BRPOP).
 * Any order-service instance can complete a reply; the waiting instance blocks on Redis.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DiscountReplyAwaiter {

    private static final String REPLY_KEY_PREFIX = "discount:reply:";

    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    @Value("${discount.reply.timeout-seconds:5}")
    private long defaultTimeoutSeconds;

    @Value("${discount.reply.key-ttl-seconds:30}")
    private long keyTtlSeconds;

    public void register(String correlationId) {
        requireCorrelationId(correlationId);
        // Drop any stale reply from a previous attempt with the same id (should not happen with UUIDs).
        redisTemplate.delete(replyKey(correlationId));
    }

    /**
     * Store reply for any waiting instance. Returns true when the payload was written.
     */
    public boolean complete(String correlationId, DiscountEvent event) {
        if (correlationId == null || correlationId.isBlank() || event == null) {
            return false;
        }
        try {
            String json = objectMapper.writeValueAsString(event);
            String key = replyKey(correlationId);
            redisTemplate.opsForList().leftPush(key, json);
            redisTemplate.expire(key, Math.max(keyTtlSeconds, defaultTimeoutSeconds + 5), TimeUnit.SECONDS);
            log.debug("Stored discount reply in Redis: correlationId={} type={}", correlationId, event.getType());
            return true;
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize discount reply: correlationId={}", correlationId, e);
            return false;
        }
    }

    public DiscountEvent await(String correlationId) throws TimeoutException, InterruptedException {
        return await(correlationId, defaultTimeoutSeconds, TimeUnit.SECONDS);
    }

    public DiscountEvent await(String correlationId, long timeout, TimeUnit unit)
        throws TimeoutException, InterruptedException {
        requireCorrelationId(correlationId);
        String key = replyKey(correlationId);

        String json = redisTemplate.opsForList().rightPop(key, timeout, unit);
        if (json == null || json.isBlank()) {
            cancel(correlationId);
            throw new TimeoutException("Timed out waiting for discount reply: " + correlationId);
        }

        try {
            return objectMapper.readValue(json, DiscountEvent.class);
        } catch (JsonProcessingException e) {
            cancel(correlationId);
            throw new IllegalStateException("Invalid discount reply payload for correlationId=" + correlationId, e);
        }
    }

    public void cancel(String correlationId) {
        if (correlationId == null || correlationId.isBlank()) {
            return;
        }
        redisTemplate.delete(replyKey(correlationId));
    }

    private static String replyKey(String correlationId) {
        return REPLY_KEY_PREFIX + correlationId;
    }

    private static void requireCorrelationId(String correlationId) {
        if (correlationId == null || correlationId.isBlank()) {
            throw new IllegalArgumentException("correlationId must not be blank");
        }
    }
}

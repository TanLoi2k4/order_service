package com.tlcn.order_service.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Cluster-safe wait for the stock-reservation saga to finish (all items OK or any failure).
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class StockReservationAwaiter {

    private static final String REPLY_KEY_PREFIX = "stock:reservation:reply:";
    private static final String RESULT_OK = "OK";
    private static final String RESULT_FAIL = "FAIL";

    private final RedisTemplate<String, String> redisTemplate;

    @Value("${stock.reservation.timeout-seconds:10}")
    private long defaultTimeoutSeconds;

    @Value("${stock.reservation.key-ttl-seconds:60}")
    private long keyTtlSeconds;

    public void register(String orderCode) {
        requireOrderCode(orderCode);
        redisTemplate.delete(replyKey(orderCode));
    }

    public boolean complete(String orderCode, boolean success) {
        if (orderCode == null || orderCode.isBlank()) {
            return false;
        }
        String key = replyKey(orderCode);
        redisTemplate.opsForList().leftPush(key, success ? RESULT_OK : RESULT_FAIL);
        redisTemplate.expire(key, Math.max(keyTtlSeconds, defaultTimeoutSeconds + 5), TimeUnit.SECONDS);
        log.debug("Stored stock reservation reply: orderCode={} success={}", orderCode, success);
        return true;
    }

    public boolean await(String orderCode) throws TimeoutException, InterruptedException {
        return await(orderCode, defaultTimeoutSeconds, TimeUnit.SECONDS);
    }

    public boolean await(String orderCode, long timeout, TimeUnit unit)
        throws TimeoutException, InterruptedException {
        requireOrderCode(orderCode);
        String json = redisTemplate.opsForList().rightPop(replyKey(orderCode), timeout, unit);
        if (json == null || json.isBlank()) {
            cancel(orderCode);
            throw new TimeoutException("Timed out waiting for stock reservation: " + orderCode);
        }
        return RESULT_OK.equals(json);
    }

    public void cancel(String orderCode) {
        if (orderCode == null || orderCode.isBlank()) {
            return;
        }
        redisTemplate.delete(replyKey(orderCode));
    }

    private static String replyKey(String orderCode) {
        return REPLY_KEY_PREFIX + orderCode;
    }

    private static void requireOrderCode(String orderCode) {
        if (orderCode == null || orderCode.isBlank()) {
            throw new IllegalArgumentException("orderCode must not be blank");
        }
    }
}

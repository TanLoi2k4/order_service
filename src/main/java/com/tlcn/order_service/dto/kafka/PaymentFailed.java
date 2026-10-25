package com.tlcn.order_service.dto.kafka;

import java.math.BigDecimal;
import java.time.Instant;

import lombok.Data;

/**
 * Event published by payment-service when payment fails
 */
@Data
public class PaymentFailed {
    private String orderCode;
    private Long orderId;
    private String customerKeycloakId;

    private BigDecimal amount;
    private String currency;

    private String provider;
    private String providerPaymentIntentId;

    private String failureCode;
    private String failureMessage;

    private Instant failedAt;
}

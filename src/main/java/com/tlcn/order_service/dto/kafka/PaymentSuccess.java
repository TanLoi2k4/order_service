package com.tlcn.order_service.dto.kafka;

import java.math.BigDecimal;
import java.time.Instant;

import lombok.Data;

@Data
public class PaymentSuccess {
    private String orderCode;
    private Long orderId;
    private String customerKeycloakId;

    private BigDecimal amount;
    private String currency;

    private String provider;
    private String providerPaymentIntentId;

    private Instant paidAt;
}

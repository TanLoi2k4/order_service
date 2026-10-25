package com.tlcn.order_service.dto;

import com.tlcn.order_service.enums.PaymentStatus;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class CreatePaymentResponse {
    private Long paymentId;
    private String orderCode;
    private PaymentStatus status;
    private String provider;
    private String providerCheckoutSessionId;
    private String providerPaymentIntentId;
    private String checkoutUrl;
}

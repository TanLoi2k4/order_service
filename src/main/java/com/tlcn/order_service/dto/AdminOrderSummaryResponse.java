package com.tlcn.order_service.dto;

import com.tlcn.order_service.enums.PaymentStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AdminOrderSummaryResponse {
    private String orderCode;
    private String customerName;
    private String customerKeycloakId;
    private List<String> vendorKeycloakIds;
    private int itemCount;
    private BigDecimal totalPayableAmount;
    /** UI bucket: pending, processing, shipped, delivered, cancelled, refund_requested */
    private String fulfillmentStatus;
    private PaymentStatus paymentStatus;
    private Instant createdAt;
}

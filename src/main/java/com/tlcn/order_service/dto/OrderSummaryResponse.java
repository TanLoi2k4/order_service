package com.tlcn.order_service.dto;

import com.tlcn.order_service.enums.PaymentStatus;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderSummaryResponse {
    private String orderCode;
    private BigDecimal totalPayableAmount;
    private PaymentStatus paymentStatus;
    private Instant createdAt;
}

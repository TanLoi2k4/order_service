package com.tlcn.order_service.dto;

import com.tlcn.order_service.enums.PaymentMethod;
import com.tlcn.order_service.enums.PaymentStatus;
import com.tlcn.order_service.enums.PayoutStatus;
import com.tlcn.order_service.enums.SubOrderStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SubOrderSummaryResponse {
    private Long id;
    private SubOrderStatus status;
    private PayoutStatus payoutStatus;
    private BigDecimal subtotal;
    private BigDecimal finalAmount;

    /** Parent order context for vendor dashboard list */
    private String orderCode;
    private Instant createdAt;
    private String customerKeycloakId;
    private String customerName;
    private String customerPhone;
    private String shippingAddress;
    private String note;
    private PaymentStatus paymentStatus;
    private PaymentMethod paymentMethod;
}

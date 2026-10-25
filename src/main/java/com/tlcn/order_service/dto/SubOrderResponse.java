package com.tlcn.order_service.dto;

import com.tlcn.order_service.enums.PaymentMethod;
import com.tlcn.order_service.enums.PaymentStatus;
import com.tlcn.order_service.enums.PayoutStatus;
import com.tlcn.order_service.enums.SubOrderStatus;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SubOrderResponse {
    private Long id;
    private String vendorKeycloakId;
    private SubOrderStatus status;

    private PayoutStatus payoutStatus;
    private String payoutRef;
    private Instant payoutAt;
    private Instant completedAt;

    private List<ReturnRequestResponse> returns;

    private BigDecimal subtotal;
    private BigDecimal finalAmount;

    private List<OrderItemResponse> items;

    /** Parent order context for vendor dashboard detail */
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

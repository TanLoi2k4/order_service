package com.tlcn.order_service.dto;

import com.tlcn.order_service.enums.PaymentMethod;
import com.tlcn.order_service.enums.PaymentStatus;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderResponse {
    private Long id;
    private Long paymentId;
    private String orderCode;
    private String customerKeycloakId;

    private String platformDiscountCode;
    private BigDecimal platformDiscountAmount;
    private Long platformDiscountHoldId;
    private BigDecimal totalPayableAmount;

    private PaymentStatus paymentStatus;
    private PaymentMethod paymentMethod;
    private String checkoutUrl;

    private String shippingAddress;
    private String customerName;
    private String customerPhone;
    private String note;

    private Instant createdAt;
    private Instant updatedAt;

    private List<SubOrderResponse> subOrders;
}

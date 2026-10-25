package com.tlcn.order_service.dto.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SubOrderCompleted {
    private Long subOrderId;
    private String vendorKeycloakId;
    private BigDecimal subTotal;
    private BigDecimal commission;
    private BigDecimal vendorPayout;

    @Builder.Default
    private List<OrderItemCompleted> items = new ArrayList<>();
}

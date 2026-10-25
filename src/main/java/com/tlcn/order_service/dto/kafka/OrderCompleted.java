package com.tlcn.order_service.dto.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderCompleted {
    private String eventId;
    private String orderCode;
    private Long orderId;
    private String customerKeycloakId;
    private String customerName;
    private String customerPhone;
    private Instant completedAt;

    @Builder.Default
    private List<SubOrderCompleted> subOrders = new ArrayList<>();
}

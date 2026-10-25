package com.tlcn.order_service.dto.kafka;

import com.tlcn.order_service.enums.ReturnRequestStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReturnRequestChanged {
    private String eventId;
    private Long returnRequestId;
    private String orderCode;
    private Long orderId;
    private Long subOrderId;
    private String customerKeycloakId;
    private String vendorKeycloakId;
    private ReturnRequestStatus status;
    private ReturnRequestStatus previousStatus;
    private BigDecimal refundAmount;
    private String reason;
    private Instant changedAt;
}

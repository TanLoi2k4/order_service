package com.tlcn.order_service.dto;

import com.tlcn.order_service.enums.ReturnReason;
import com.tlcn.order_service.enums.ReturnRequestStatus;
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
public class ReturnRequestResponse {
    private Long id;
    private Long subOrderId;
    private String orderCode;
    private String customerKeycloakId;
    private String vendorKeycloakId;
    private ReturnRequestStatus status;
    private ReturnReason reason;
    private String description;
    private String sellerNote;
    private String customerShipNote;
    private String adminNote;
    private BigDecimal refundAmount;
    private String currency;
    private List<ReturnRequestItemResponse> items;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant approvedAt;
    private Instant shippedAt;
    private Instant receivedAt;
    private Instant refundedAt;
    private Instant sellerRespondBy;
    private Long returnShipmentId;
    private String returnExternalShipmentId;
    private String returnCarrierCode;
}

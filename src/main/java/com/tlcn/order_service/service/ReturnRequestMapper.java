package com.tlcn.order_service.service;

import com.tlcn.order_service.dto.ReturnRequestItemResponse;
import com.tlcn.order_service.dto.ReturnRequestResponse;
import com.tlcn.order_service.enums.ReturnRequestStatus;
import com.tlcn.order_service.model.ReturnRequest;
import com.tlcn.order_service.model.ReturnRequestItem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Component
public class ReturnRequestMapper {

    @Value("${return.auto-approve-hours:48}")
    private long autoApproveHours;

    public ReturnRequestResponse toResponse(ReturnRequest request) {
        if (request == null) {
            return null;
        }

        List<ReturnRequestItemResponse> items = Optional.ofNullable(request.getItems()).orElseGet(List::of).stream()
            .map(this::toItemResponse)
            .toList();

        Instant sellerRespondBy = null;
        if (request.getStatus() == ReturnRequestStatus.REQUESTED && request.getCreatedAt() != null) {
            sellerRespondBy = request.getCreatedAt().plus(Duration.ofHours(Math.max(1L, autoApproveHours)));
        }

        Long subOrderId = request.getSubOrder() != null ? request.getSubOrder().getId() : null;

        return ReturnRequestResponse.builder()
            .id(request.getId())
            .subOrderId(subOrderId)
            .orderCode(request.getOrderCode())
            .customerKeycloakId(request.getCustomerKeycloakId())
            .vendorKeycloakId(request.getVendorKeycloakId())
            .status(request.getStatus())
            .reason(request.getReason())
            .description(request.getDescription())
            .sellerNote(request.getSellerNote())
            .customerShipNote(request.getCustomerShipNote())
            .adminNote(request.getAdminNote())
            .refundAmount(request.getRefundAmount())
            .currency(request.getCurrency())
            .items(items)
            .createdAt(request.getCreatedAt())
            .updatedAt(request.getUpdatedAt())
            .approvedAt(request.getApprovedAt())
            .shippedAt(request.getShippedAt())
            .receivedAt(request.getReceivedAt())
            .refundedAt(request.getRefundedAt())
            .sellerRespondBy(sellerRespondBy)
            .returnShipmentId(request.getReturnShipmentId())
            .returnExternalShipmentId(request.getReturnExternalShipmentId())
            .returnCarrierCode(request.getReturnCarrierCode())
            .build();
    }

    private ReturnRequestItemResponse toItemResponse(ReturnRequestItem item) {
        return ReturnRequestItemResponse.builder()
            .id(item.getId())
            .orderItemId(item.getOrderItemId())
            .productId(item.getProductId())
            .quantity(item.getQuantity())
            .unitPrice(item.getUnitPrice())
            .lineTotal(item.getLineTotal())
            .build();
    }
}

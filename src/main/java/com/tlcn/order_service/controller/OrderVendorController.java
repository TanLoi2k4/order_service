package com.tlcn.order_service.controller;

import com.tlcn.order_service.dto.PageResponse;
import com.tlcn.order_service.dto.ShipmentRequest;
import com.tlcn.order_service.dto.ResponseDTO;
import com.tlcn.order_service.dto.SubOrderResponse;
import com.tlcn.order_service.dto.SubOrderSummaryResponse;
import com.tlcn.order_service.dto.VendorSubOrderStatsResponse;
import com.tlcn.order_service.dto.UpdateOrderItemStatusRequest;
import com.tlcn.order_service.dto.UpdateSubOrderStatusRequest;
import com.tlcn.order_service.enums.StatsPeriod;
import com.tlcn.order_service.enums.SubOrderStatus;
import com.tlcn.order_service.service.VendorOrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/orders/vendor")
@RequiredArgsConstructor
@PreAuthorize("hasRole('VENDOR')")
public class OrderVendorController {

    private final VendorOrderService vendorOrderService;

    @GetMapping
    public ResponseEntity<ResponseDTO<PageResponse<SubOrderSummaryResponse>>> getMySubOrders(
        Authentication authentication,
        @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "10") int size,
        @RequestParam(required = false) SubOrderStatus status,
        @RequestParam(required = false) String customerKeycloakId,
        @RequestParam(required = false) String q
    ) {
        PageResponse<SubOrderSummaryResponse> result = vendorOrderService.getMySubOrders(
            authentication.getName(),
            page,
            size,
            status,
            customerKeycloakId,
            q
        );
        return ResponseEntity.ok(new ResponseDTO<>(true, "Sub-orders retrieved successfully", result));
    }

    @GetMapping("/stats")
    public ResponseEntity<ResponseDTO<VendorSubOrderStatsResponse>> getMySubOrderStats(
        Authentication authentication,
        @RequestParam(defaultValue = "ALL") StatsPeriod period,
        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate anchor
    ) {
        VendorSubOrderStatsResponse result = vendorOrderService.getMySubOrderStats(
            authentication.getName(),
            period,
            anchor
        );
        return ResponseEntity.ok(new ResponseDTO<>(true, "Sub-order stats retrieved successfully", result));
    }

    @GetMapping("/{subOrderId}")
    public ResponseEntity<ResponseDTO<SubOrderResponse>> getMySubOrderDetail(
        Authentication authentication,
        @PathVariable Long subOrderId
    ) {
        SubOrderResponse result = vendorOrderService.getMySubOrderDetail(authentication.getName(), subOrderId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Sub-order retrieved successfully", result));
    }

    @PutMapping("/{subOrderId}/status")
    public ResponseEntity<ResponseDTO<SubOrderResponse>> updateMySubOrderStatus(
        Authentication authentication,
        @PathVariable Long subOrderId,
        @Valid @RequestBody UpdateSubOrderStatusRequest request,
        HttpServletRequest httpServletRequest
    ) {
        String authorization = httpServletRequest.getHeader("Authorization");
        SubOrderResponse result = vendorOrderService.updateMySubOrderStatus(authentication.getName(), subOrderId, request.getStatus(), authorization);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Sub-order status updated successfully", result));
    }

    @PutMapping("/{subOrderId}/items/{itemId}/status")
    public ResponseEntity<ResponseDTO<SubOrderResponse>> updateMyOrderItemStatus(
        Authentication authentication,
        @PathVariable Long subOrderId,
        @PathVariable Long itemId,
        @Valid @RequestBody UpdateOrderItemStatusRequest request,
        HttpServletRequest httpServletRequest
    ) {
        String authorization = httpServletRequest.getHeader("Authorization");
        SubOrderResponse result = vendorOrderService.updateMyOrderItemStatus(authentication.getName(), subOrderId, itemId, request.getStatus(), authorization);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Order item status updated successfully", result));
    }
    
    @PostMapping("/{subOrderId}/shipping")
    public ResponseEntity<ResponseDTO<SubOrderResponse>> requestShipment(
        Authentication authentication,
        @PathVariable Long subOrderId,
        @Valid @RequestBody ShipmentRequest request
    ) {
        SubOrderResponse result = vendorOrderService.requestShipment(
            authentication.getName(),
            subOrderId,
            request.getServiceLevel(),
            request.getCarrierPreference(),
            request.getPickupAddress()
        );
        return ResponseEntity.ok(new ResponseDTO<>(true, "Shipment request published successfully", result));
    }
}

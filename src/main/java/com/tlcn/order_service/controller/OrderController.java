package com.tlcn.order_service.controller;

import com.tlcn.order_service.dto.OrderRequestDTO;
import com.tlcn.order_service.dto.OrderResponseDTO;
import com.tlcn.order_service.dto.ResponseDTO;
import com.tlcn.order_service.model.Status;
import com.tlcn.order_service.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {
    private final OrderService orderService;

    @PostMapping
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<OrderResponseDTO>> createOrder(
            @Valid @RequestBody OrderRequestDTO request,
            Authentication authentication) {
        String keycloakId = authentication.getName();
        OrderResponseDTO response = orderService.createOrder(request, keycloakId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Order created successfully", response));
    }

    @PatchMapping("/{orderId}/status")
    @PreAuthorize("hasAnyRole('CUSTOMER', 'VENDOR')")
    public ResponseEntity<ResponseDTO<OrderResponseDTO>> updateOrderStatus(
            @PathVariable Long orderId,
            @RequestBody Map<String, String> statusUpdate,
            Authentication authentication) {
        String keycloakId = authentication.getName();
        if (statusUpdate.get("status") == null) {
             throw new OrderService.CustomException("Status field is required");
        }
        Status newStatus = Status.valueOf(statusUpdate.get("status").toUpperCase());
        OrderResponseDTO response = orderService.updateOrderStatus(orderId, newStatus, keycloakId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Order status updated successfully", response));
    }

    @GetMapping("/{orderId}")
    @PreAuthorize("hasAnyRole('CUSTOMER', 'VENDOR')")
    public ResponseEntity<ResponseDTO<OrderResponseDTO>> getOrderDetails(
            @PathVariable Long orderId,
            Authentication authentication) {
        String keycloakId = authentication.getName();
        OrderResponseDTO order = orderService.getOrderDetails(orderId, keycloakId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Order retrieved successfully", order));
    }

    @GetMapping("/customer/my")
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<List<OrderResponseDTO>>> getOrdersByCustomerKeycloakId(
            Authentication authentication) {
        String keycloakId = authentication.getName();
        List<OrderResponseDTO> orders = orderService.getOrdersByCustomerKeycloakId(keycloakId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Customer orders retrieved successfully", orders));
    }

    @GetMapping("/vendor/my")
    @PreAuthorize("hasRole('VENDOR')")
    public ResponseEntity<ResponseDTO<List<OrderResponseDTO>>> getOrdersByVendorKeycloakId(
            Authentication authentication) {
        String keycloakId = authentication.getName();
        List<OrderResponseDTO> orders = orderService.getOrdersByVendorKeycloakId(keycloakId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Vendor orders retrieved successfully", orders));
    }
}
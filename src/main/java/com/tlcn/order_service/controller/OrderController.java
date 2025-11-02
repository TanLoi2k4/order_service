package com.tlcn.order_service.controller;

import com.tlcn.order_service.dto.OrderDTO;
import com.tlcn.order_service.dto.ResponseDTO;
import com.tlcn.order_service.model.Order;
import com.tlcn.order_service.model.OrderItem;
import com.tlcn.order_service.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    @Autowired
    private OrderService orderService;

    /* ===================== CUSTOMER APIs ===================== */

    @PostMapping
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<Order>> createOrder(
            @Valid @RequestBody OrderDTO orderDTO,
            Authentication authentication) {
        String keycloakId = authentication.getName();
        Order order = orderService.createOrder(orderDTO, keycloakId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Order created successfully", order));
    }

    @GetMapping("/{id}")
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<Order>> getOrder(
            @PathVariable Long id,
            Authentication authentication) {
        String keycloakId = authentication.getName();
        Order order = orderService.getOrder(id, keycloakId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Order retrieved successfully", order));
    }

    @GetMapping
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<Page<Order>>> getOrdersByCustomer(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size,
            Authentication authentication) {
        String keycloakId = authentication.getName();
        Pageable pageable = PageRequest.of(page, size);
        Page<Order> orders = orderService.getOrdersByCustomer(keycloakId, pageable);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Orders retrieved successfully", orders));
    }

    @PutMapping("/cancel/{id}")
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<Void>> cancelOrder(
            @PathVariable Long id,
            Authentication authentication) {
        String keycloakId = authentication.getName();
        orderService.cancelOrder(id, keycloakId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Order cancelled successfully", null));
    }

    /* ===================== VENDOR APIs ===================== */

    @GetMapping("/vendor/items")
    @PreAuthorize("hasRole('VENDOR')")
    public ResponseEntity<ResponseDTO<Page<OrderItem>>> getOrderItemsByVendor(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size,
            Authentication authentication) {
        String keycloakId = authentication.getName();
        Pageable pageable = PageRequest.of(page, size);
        Page<OrderItem> items = orderService.getOrdersByVendor(keycloakId, pageable);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Vendor order items retrieved successfully", items));
    }

    @GetMapping("/vendor/items/{itemId}")
    @PreAuthorize("hasRole('VENDOR')")
    public ResponseEntity<ResponseDTO<OrderItem>> getVendorOrderItem(
            @PathVariable Long itemId,
            Authentication authentication) {
        String keycloakId = authentication.getName();
        OrderItem item = orderService.getVendorOrderItem(itemId, keycloakId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Vendor order item retrieved successfully", item));
    }

    @PutMapping("/vendor/items/status/{itemId}")
    @PreAuthorize("hasRole('VENDOR')")
    public ResponseEntity<ResponseDTO<Void>> updateItemStatus(
            @PathVariable Long itemId,
            @RequestParam String status,
            Authentication authentication) {
        String keycloakId = authentication.getName();
        orderService.updateItemStatus(itemId, keycloakId, status);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Item status updated successfully", null));
    }

    @PutMapping("/vendor/items/cancel/{itemId}")
    @PreAuthorize("hasRole('VENDOR')")
    public ResponseEntity<ResponseDTO<Void>> cancelVendorItem(
            @PathVariable Long itemId,
            Authentication authentication) {
        String keycloakId = authentication.getName();
        orderService.cancelVendorItem(itemId, keycloakId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Vendor item cancelled successfully", null));
    }
}
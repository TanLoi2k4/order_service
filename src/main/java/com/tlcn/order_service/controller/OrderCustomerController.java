package com.tlcn.order_service.controller;

import com.tlcn.order_service.dto.CreateOrderRequest;
import com.tlcn.order_service.dto.OrderResponse;
import com.tlcn.order_service.dto.OrderSummaryResponse;
import com.tlcn.order_service.dto.PageResponse;
import com.tlcn.order_service.dto.ResponseDTO;
import com.tlcn.order_service.dto.ReviewEligibilityResponse;
import com.tlcn.order_service.service.CustomerOrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderCustomerController {

    private final CustomerOrderService customerOrderService;

    @PostMapping
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<OrderResponse>> createOrder(
        @Valid @RequestBody CreateOrderRequest request,
        Authentication authentication,
        HttpServletRequest httpServletRequest
    ) {
        String keycloakId = authentication.getName();
        String authorization = httpServletRequest.getHeader("Authorization");
        OrderResponse result = customerOrderService.createOrderFromCart(keycloakId, request, authorization);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Order created successfully", result));
    }

    @GetMapping
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<PageResponse<OrderSummaryResponse>>> myOrders(
        @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "10") int size,
        Authentication authentication
    ) {
        String keycloakId = authentication.getName();
        PageResponse<OrderSummaryResponse> result = customerOrderService.getMyOrders(keycloakId, page, size);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Orders retrieved successfully", result));
    }

    @GetMapping("/verify-purchase")
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<Boolean>> verifyPurchase(
            @RequestParam Long productId,
            Authentication authentication) {
        String customerKeycloakId = authentication.getName();
        boolean verified = customerOrderService.verifyPurchase(customerKeycloakId, productId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Purchase verification completed", verified));
    }

    @GetMapping("/review-eligibility")
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<ReviewEligibilityResponse>> reviewEligibility(
            @RequestParam Long productId,
            Authentication authentication) {
        String customerKeycloakId = authentication.getName();
        ReviewEligibilityResponse result = customerOrderService.getReviewEligibility(customerKeycloakId, productId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Review eligibility retrieved", result));
    }

    @GetMapping("/{orderCode}")
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<OrderResponse>> myOrderDetail(
        @PathVariable String orderCode,
        Authentication authentication
    ) {
        String keycloakId = authentication.getName();
        OrderResponse order = customerOrderService.getMyOrderDetail(keycloakId, orderCode);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Order retrieved successfully", order));
    }

    @PostMapping("/{orderCode}/cancel")
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<OrderResponse>> cancelMyOrder(
        @PathVariable String orderCode,
        Authentication authentication,
        HttpServletRequest httpServletRequest
    ) {
        String keycloakId = authentication.getName();
        String authorization = httpServletRequest.getHeader("Authorization");
        OrderResponse order = customerOrderService.cancelMyOrder(keycloakId, orderCode, authorization);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Order cancelled successfully", order));
    }

    @PostMapping("/{orderCode}/items/{itemId}/cancel")
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<OrderResponse>> cancelMyOrderItem(
        @PathVariable String orderCode,
        @PathVariable Long itemId,
        Authentication authentication,
        HttpServletRequest httpServletRequest
    ) {
        String keycloakId = authentication.getName();
        String authorization = httpServletRequest.getHeader("Authorization");
        OrderResponse order = customerOrderService.cancelMyOrderItem(keycloakId, orderCode, itemId, authorization);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Order item cancelled successfully", order));
    }
}

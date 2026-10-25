package com.tlcn.order_service.controller;

import com.tlcn.order_service.dto.AdminOrderDetailResponse;
import com.tlcn.order_service.dto.AdminOrderStatsResponse;
import com.tlcn.order_service.dto.AdminOrderSummaryResponse;
import com.tlcn.order_service.dto.PageResponse;
import com.tlcn.order_service.dto.ResponseDTO;
import com.tlcn.order_service.enums.PaymentStatus;
import com.tlcn.order_service.enums.StatsPeriod;
import com.tlcn.order_service.service.AdminOrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

@RestController
@RequestMapping("/orders/admin")
@RequiredArgsConstructor
@PreAuthorize("hasRole('ADMIN') or hasRole('SUPER_ADMIN')")
public class AdminOrderController {

    private final AdminOrderService adminOrderService;

    @GetMapping
    public ResponseEntity<ResponseDTO<PageResponse<AdminOrderSummaryResponse>>> listOrders(
        @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "10") int size,
        @RequestParam(required = false) String search,
        @RequestParam(required = false) String status,
        @RequestParam(required = false) PaymentStatus paymentStatus
    ) {
        PageResponse<AdminOrderSummaryResponse> result = adminOrderService.listOrders(
            page,
            size,
            search,
            status,
            paymentStatus
        );
        return ResponseEntity.ok(new ResponseDTO<>(true, "Orders retrieved successfully", result));
    }

    @GetMapping("/stats")
    public ResponseEntity<ResponseDTO<AdminOrderStatsResponse>> getStats(
        @RequestParam(defaultValue = "ALL") StatsPeriod period,
        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate anchor
    ) {
        AdminOrderStatsResponse stats = adminOrderService.getStats(period, anchor);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Order stats retrieved successfully", stats));
    }

    @GetMapping("/{orderCode}")
    public ResponseEntity<ResponseDTO<AdminOrderDetailResponse>> getOrderDetail(@PathVariable String orderCode) {
        AdminOrderDetailResponse detail = adminOrderService.getOrderDetail(orderCode);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Order retrieved successfully", detail));
    }
}

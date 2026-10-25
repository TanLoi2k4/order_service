package com.tlcn.order_service.controller;

import com.tlcn.order_service.dto.CreateReturnRequest;
import com.tlcn.order_service.dto.ResponseDTO;
import com.tlcn.order_service.dto.ReturnRequestResponse;
import com.tlcn.order_service.service.ReturnRequestService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
@PreAuthorize("hasRole('CUSTOMER')")
public class OrderCustomerReturnController {

    private final ReturnRequestService returnRequestService;

    @PostMapping("/{orderCode}/sub-orders/{subOrderId}/returns")
    public ResponseEntity<ResponseDTO<ReturnRequestResponse>> createReturn(
        @PathVariable String orderCode,
        @PathVariable Long subOrderId,
        @Valid @RequestBody CreateReturnRequest request,
        Authentication authentication
    ) {
        ReturnRequestResponse result = returnRequestService.create(
            authentication.getName(),
            orderCode,
            subOrderId,
            request
        );
        return ResponseEntity.ok(new ResponseDTO<>(true, "Return request created successfully", result));
    }

    @GetMapping("/{orderCode}/returns")
    public ResponseEntity<ResponseDTO<List<ReturnRequestResponse>>> listReturns(
        @PathVariable String orderCode,
        Authentication authentication
    ) {
        List<ReturnRequestResponse> result = returnRequestService.listForCustomer(authentication.getName(), orderCode);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Return requests retrieved successfully", result));
    }

    @PostMapping("/{orderCode}/returns/{returnId}/escalate")
    public ResponseEntity<ResponseDTO<ReturnRequestResponse>> escalate(
        @PathVariable String orderCode,
        @PathVariable Long returnId,
        Authentication authentication
    ) {
        ReturnRequestResponse result = returnRequestService.escalate(authentication.getName(), orderCode, returnId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Return escalated to admin", result));
    }
}

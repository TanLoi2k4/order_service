package com.tlcn.order_service.controller;

import com.tlcn.order_service.dto.PageResponse;
import com.tlcn.order_service.dto.ResponseDTO;
import com.tlcn.order_service.dto.ReturnActionRequest;
import com.tlcn.order_service.dto.ReturnRequestResponse;
import com.tlcn.order_service.enums.ReturnRequestStatus;
import com.tlcn.order_service.service.ReturnRequestService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders/vendor/returns")
@RequiredArgsConstructor
@PreAuthorize("hasRole('VENDOR')")
public class OrderVendorReturnController {

    private final ReturnRequestService returnRequestService;

    @GetMapping
    public ResponseEntity<ResponseDTO<PageResponse<ReturnRequestResponse>>> listReturns(
        Authentication authentication,
        @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "10") int size,
        @RequestParam(required = false) ReturnRequestStatus status
    ) {
        PageResponse<ReturnRequestResponse> result = returnRequestService.listForVendor(
            authentication.getName(),
            status,
            page,
            size
        );
        return ResponseEntity.ok(new ResponseDTO<>(true, "Return requests retrieved successfully", result));
    }

    @GetMapping("/{returnId}")
    public ResponseEntity<ResponseDTO<ReturnRequestResponse>> getReturn(
        Authentication authentication,
        @PathVariable Long returnId
    ) {
        ReturnRequestResponse result = returnRequestService.getForVendor(authentication.getName(), returnId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Return request retrieved successfully", result));
    }

    @PostMapping("/{returnId}/approve")
    public ResponseEntity<ResponseDTO<ReturnRequestResponse>> approve(
        Authentication authentication,
        @PathVariable Long returnId,
        @RequestBody(required = false) ReturnActionRequest request
    ) {
        ReturnRequestResponse result = returnRequestService.approveByVendor(
            authentication.getName(),
            returnId,
            request != null ? request.getNote() : null
        );
        return ResponseEntity.ok(new ResponseDTO<>(true, "Return request approved", result));
    }

    @PostMapping("/{returnId}/reject")
    public ResponseEntity<ResponseDTO<ReturnRequestResponse>> reject(
        Authentication authentication,
        @PathVariable Long returnId,
        @Valid @RequestBody ReturnActionRequest request
    ) {
        ReturnRequestResponse result = returnRequestService.rejectByVendor(
            authentication.getName(),
            returnId,
            request != null ? request.getNote() : null
        );
        return ResponseEntity.ok(new ResponseDTO<>(true, "Return request rejected", result));
    }
}

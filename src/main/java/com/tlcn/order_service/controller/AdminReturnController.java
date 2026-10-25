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
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders/admin/returns")
@RequiredArgsConstructor
@PreAuthorize("hasRole('ADMIN') or hasRole('SUPER_ADMIN')")
public class AdminReturnController {

    private final ReturnRequestService returnRequestService;

    @GetMapping
    public ResponseEntity<ResponseDTO<PageResponse<ReturnRequestResponse>>> listReturns(
        @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "10") int size,
        @RequestParam(required = false) ReturnRequestStatus status
    ) {
        PageResponse<ReturnRequestResponse> result = returnRequestService.listForAdmin(status, page, size);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Return requests retrieved successfully", result));
    }

    @PostMapping("/{returnId}/approve")
    public ResponseEntity<ResponseDTO<ReturnRequestResponse>> approve(
        @PathVariable Long returnId,
        @RequestBody(required = false) ReturnActionRequest request
    ) {
        ReturnRequestResponse result = returnRequestService.approveByAdmin(
            returnId,
            request != null ? request.getNote() : null
        );
        return ResponseEntity.ok(new ResponseDTO<>(true, "Return request approved by admin", result));
    }

    @PostMapping("/{returnId}/reject")
    public ResponseEntity<ResponseDTO<ReturnRequestResponse>> reject(
        @PathVariable Long returnId,
        @Valid @RequestBody ReturnActionRequest request
    ) {
        ReturnRequestResponse result = returnRequestService.rejectByAdmin(
            returnId,
            request != null ? request.getNote() : null
        );
        return ResponseEntity.ok(new ResponseDTO<>(true, "Return request closed by admin", result));
    }
}

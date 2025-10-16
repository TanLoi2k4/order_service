package com.tlcn.order_service.dto;

import com.tlcn.order_service.model.Status;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Data
public class OrderResponseDTO {
    private Long id;
    private Long customerId;
    private Long vendorId;
    private BigDecimal totalAmount;
    private Status status;
    private String shippingAddress;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private List<OrderItemDTO> items;
}
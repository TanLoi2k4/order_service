package com.tlcn.order_service.dto;

import com.tlcn.order_service.model.Status;
import jakarta.validation.constraints.*;
import lombok.Data;

import java.math.BigDecimal;
import java.util.List;

@Data
public class OrderRequestDTO {
    @NotNull(message = "Customer ID must not be null")
    @Positive(message = "Customer ID must be positive")
    private Long customerId;

    @NotNull(message = "Vendor ID must not be null")
    @Positive(message = "Vendor ID must be positive")
    private Long vendorId;

    @NotNull(message = "Total amount must not be null")
    @PositiveOrZero(message = "Total amount must be non-negative")
    private BigDecimal totalAmount;

    @NotBlank(message = "Shipping address must not be blank")
    @Size(max = 500, message = "Shipping address must not exceed 500 characters")
    private String shippingAddress;

    @NotEmpty(message = "Order items must not be empty")
    @Size(min = 1, max = 50, message = "Order must have between 1 and 50 items")
    private List<OrderItemDTO> items;
}
package com.tlcn.order_service.dto;

import jakarta.validation.constraints.*;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class OrderItemDTO {
    @NotNull(message = "Product ID must not be null")
    @Positive(message = "Product ID must be positive")
    private Long productId;

    @NotNull(message = "Quantity must not be null")
    @Positive(message = "Quantity must be positive")
    @Max(value = 100, message = "Quantity must not exceed 100")
    private Integer quantity;

    @NotNull(message = "Price must not be null")
    @PositiveOrZero(message = "Price must be non-negative")
    private BigDecimal price;
}
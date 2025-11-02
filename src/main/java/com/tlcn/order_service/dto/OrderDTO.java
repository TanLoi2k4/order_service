package com.tlcn.order_service.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OrderDTO {
    @NotBlank(message = "Shipping address is required")
    private String shippingAddress;
}
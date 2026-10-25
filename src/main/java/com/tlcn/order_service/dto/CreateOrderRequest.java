package com.tlcn.order_service.dto;

import com.tlcn.order_service.enums.PaymentMethod;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CreateOrderRequest {

    @NotBlank(message = "Shipping address is required")
    private String shippingAddress;

    @NotBlank(message = "Customer name is required")
    private String customerName;

    @NotBlank(message = "Customer phone is required")
    private String customerPhone;

    private String note;

    @NotNull(message = "paymentMethod is required")
    private PaymentMethod paymentMethod;

    // Voucher/discount applied at platform level (optional)
    private String platformDiscountCode;
}

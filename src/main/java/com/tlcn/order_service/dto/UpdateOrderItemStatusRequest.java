package com.tlcn.order_service.dto;

import com.tlcn.order_service.enums.OrderItemStatus;
import jakarta.validation.constraints.NotNull;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UpdateOrderItemStatusRequest {

    @NotNull(message = "Order item status is required")
    private OrderItemStatus status;
}

package com.tlcn.order_service.dto;

import com.tlcn.order_service.enums.SubOrderStatus;
import jakarta.validation.constraints.NotNull;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UpdateSubOrderStatusRequest {

    @NotNull(message = "Status is required")
    private SubOrderStatus status;
}

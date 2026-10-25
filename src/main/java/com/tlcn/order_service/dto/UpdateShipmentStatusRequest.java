package com.tlcn.order_service.dto;

import com.tlcn.order_service.enums.ShipmentStatus;
import jakarta.validation.constraints.NotNull;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UpdateShipmentStatusRequest {

    @NotNull(message = "Shipment status is required")
    private ShipmentStatus status;
}

package com.tlcn.order_service.dto;

import com.tlcn.order_service.enums.ShipmentStatus;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ShipmentResponse {
    private Long id;
    private ShipmentStatus status;
}

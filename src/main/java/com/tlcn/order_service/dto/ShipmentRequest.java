package com.tlcn.order_service.dto;

import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ShipmentRequest {

    @Size(max = 500)
    private String pickupAddress;

    @Size(max = 100)
    private String serviceLevel;

    @Size(max = 100)
    private String carrierPreference;
}

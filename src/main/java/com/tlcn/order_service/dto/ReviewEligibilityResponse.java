package com.tlcn.order_service.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ReviewEligibilityResponse {
    private boolean purchased;
    private Long orderId;
}

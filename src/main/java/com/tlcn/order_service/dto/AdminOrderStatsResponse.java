package com.tlcn.order_service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AdminOrderStatsResponse {
    private long totalOrders;
    private long pending;
    private long processing;
    private long shipped;
    private long delivered;
    private long cancelled;
    private long refundRequested;
}

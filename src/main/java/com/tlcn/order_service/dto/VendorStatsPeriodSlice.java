package com.tlcn.order_service.dto;

import com.tlcn.order_service.enums.StatsPeriod;
import com.tlcn.order_service.enums.SubOrderStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class VendorStatsPeriodSlice {
    private StatsPeriod granularity;
    private Instant from;
    private Instant to;
    private String label;
    private long total;

    @Builder.Default
    private Map<SubOrderStatus, Long> byStatus = new EnumMap<>(SubOrderStatus.class);
}

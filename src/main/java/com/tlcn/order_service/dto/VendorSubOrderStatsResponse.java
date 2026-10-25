package com.tlcn.order_service.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.tlcn.order_service.enums.SubOrderStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.EnumMap;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class VendorSubOrderStatsResponse {
    private long total;

    @Builder.Default
    private Map<SubOrderStatus, Long> byStatus = new EnumMap<>(SubOrderStatus.class);

    private VendorStatsPeriodSlice period;
}

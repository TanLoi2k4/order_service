package com.tlcn.order_service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CoPurchaseCountDto {
    private Long sourceProductId;
    private Long targetProductId;
    private Long customerCount;
}

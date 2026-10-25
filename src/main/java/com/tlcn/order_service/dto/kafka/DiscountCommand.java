package com.tlcn.order_service.dto.kafka;

import java.time.Instant;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DiscountCommand {
    private String commandId;
    private DiscountCommandType type;

    private Long orderId;
    private String userId;
    private String code;

    private Long holdId;

    private Double subtotal;
    private Instant createdAt;

    private String reason;
}

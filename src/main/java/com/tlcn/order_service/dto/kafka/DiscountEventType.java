package com.tlcn.order_service.dto.kafka;

public enum DiscountEventType {
    HELD,
    HOLD_FAILED,
    COMMITTED,
    COMMIT_FAILED,
    RELEASED,
    RELEASE_FAILED,
    EXPIRED
}

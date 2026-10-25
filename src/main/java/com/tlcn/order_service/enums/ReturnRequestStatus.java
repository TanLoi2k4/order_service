package com.tlcn.order_service.enums;

import java.util.EnumSet;
import java.util.Set;

public enum ReturnRequestStatus {
    REQUESTED,
    APPROVED,
    SHIPPED,
    RECEIVED,
    REFUNDED,
    REJECTED,
    DISPUTED,
    CLOSED;

    public static final Set<ReturnRequestStatus> OPEN = EnumSet.of(
        REQUESTED,
        APPROVED,
        SHIPPED,
        DISPUTED
    );

    public boolean isOpen() {
        return OPEN.contains(this);
    }

    public boolean isTerminal() {
        return this == REFUNDED || this == CLOSED;
    }
}

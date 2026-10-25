package com.tlcn.order_service.enums;

public enum ShipmentStatus {
    CREATED,
    BOOKING_FAILED,
    BOOKED,
    PICKED_UP,
    IN_TRANSIT,
    DELIVERED,
    FAILED_DELIVERY,
    RETURNING,
    RETURNED,
    CANCELLED;

    public int progressRank() {
        return switch (this) {
            case CREATED, BOOKING_FAILED -> 0;
            case BOOKED -> 1;
            case PICKED_UP -> 2;
            case IN_TRANSIT, FAILED_DELIVERY -> 3;
            case DELIVERED, RETURNING -> 4;
            case RETURNED, CANCELLED -> 5;
        };
    }
}

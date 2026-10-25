package com.tlcn.order_service.enums;

public enum SubOrderStatus {
    CREATED,           // Đơn mới
    PROCESSING,        // Vendor đang chuẩn bị (đã xác nhận item xong)
    SHIPPING_REQUESTED, // Vendor đã gửi yêu cầu vận chuyển, chờ carrier booking
    SHIPPING,          // Đã bàn giao cho bên vận chuyển; trạng thái giao hàng đang thực sự diễn ra
    RETURNING,         // Đơn đang được hoàn trả về vendor
    COMPLETED,         // Đã hoàn tất (Khách nhận hàng + Hết thời gian khiếu nại)
    CANCELLED,         // Đơn bị hủy (Khách hủy trước khi Vendor xác nhận, hoặc Vendor hủy sau khi xác nhận)
    RETURNED;          // Khách trả hàng

    public int progressRank() {
        return switch (this) {
            case CREATED -> 0;
            case PROCESSING -> 1;
            case SHIPPING_REQUESTED -> 2;
            case SHIPPING -> 3;
            case RETURNING -> 4;
            case COMPLETED, CANCELLED, RETURNED -> 5;
        };
    }

    public boolean isTerminal() {
        return this == COMPLETED || this == CANCELLED || this == RETURNED;
    }
}

package com.tlcn.order_service.enums;

public enum OrderItemStatus {
    PENDING,    // Chờ Vendor xử lý
    CONFIRMED,  // Vendor xác nhận còn hàng và sẽ giao
    CANCELLED,  // Vendor hủy (hết hàng/lỗi)
    RETURNED    // Khách trả hàng
}
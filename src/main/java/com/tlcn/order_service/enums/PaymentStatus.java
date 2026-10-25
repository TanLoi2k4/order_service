package com.tlcn.order_service.enums;

public enum PaymentStatus {
    PENDING,    // Chờ thanh toán (với Chuyển khoản/Ví điện tử)
    COMPLETED,  // Đã thanh toán thành công
    CANCELLED,  // Đã hủy (trước khi thanh toán)
    FAILED,     // Thanh toán thất bại
    REFUNDED,   // Đã hoàn tiền (khi hủy đơn)
    PARTIALLY_REFUNDED // Hoàn tiền một phần (khi hủy 1 vài món trong SubOrder)
}
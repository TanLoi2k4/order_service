package com.tlcn.order_service.service;

import com.tlcn.order_service.enums.PaymentStatus;
import com.tlcn.order_service.enums.SubOrderStatus;
import com.tlcn.order_service.model.Order;
import com.tlcn.order_service.model.SubOrder;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

public final class AdminOrderStatusSupport {

    private AdminOrderStatusSupport() {
    }

    public static String deriveFulfillmentStatus(Order order) {
        if (order == null) {
            return "pending";
        }

        PaymentStatus paymentStatus = order.getPaymentStatus();
        if (paymentStatus == PaymentStatus.REFUNDED || paymentStatus == PaymentStatus.PARTIALLY_REFUNDED) {
            return "refund_requested";
        }

        List<SubOrder> subOrders = Optional.ofNullable(order.getSubOrders()).orElse(List.of());
        if (subOrders.isEmpty()) {
            return "pending";
        }

        if (subOrders.stream().anyMatch(so -> so.getStatus() == SubOrderStatus.RETURNED)) {
            return "refund_requested";
        }

        if (subOrders.stream().allMatch(so -> so.getStatus() == SubOrderStatus.CANCELLED)) {
            return "cancelled";
        }

        if (subOrders.stream().allMatch(so -> so.getStatus() == SubOrderStatus.COMPLETED)) {
            return "delivered";
        }

        if (subOrders.stream().anyMatch(so ->
            so.getStatus() == SubOrderStatus.SHIPPING || so.getStatus() == SubOrderStatus.RETURNING
        )) {
            return "shipped";
        }

        if (subOrders.stream().anyMatch(so ->
            so.getStatus() == SubOrderStatus.PROCESSING || so.getStatus() == SubOrderStatus.SHIPPING_REQUESTED
        )) {
            return "processing";
        }

        return "pending";
    }

    public static boolean matchesFulfillmentFilter(Order order, String statusFilter) {
        if (statusFilter == null || statusFilter.isBlank()) {
            return true;
        }
        return deriveFulfillmentStatus(order).equals(statusFilter.trim().toLowerCase(Locale.ROOT));
    }
}

package com.tlcn.order_service.service;

import com.tlcn.order_service.dto.CreatePaymentResponse;
import com.tlcn.order_service.dto.OrderItemResponse;
import com.tlcn.order_service.dto.OrderResponse;
import com.tlcn.order_service.dto.ReturnRequestResponse;
import com.tlcn.order_service.dto.SubOrderResponse;
import com.tlcn.order_service.dto.SubOrderSummaryResponse;
import com.tlcn.order_service.model.Order;
import com.tlcn.order_service.model.OrderItem;
import com.tlcn.order_service.model.SubOrder;
import com.tlcn.order_service.repository.ReturnRequestRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class OrderResponseMapper {

    private final ReturnRequestRepository returnRequestRepository;
    private final ReturnRequestMapper returnRequestMapper;

    public OrderResponse toOrderResponse(Order order) {
        return toOrderResponse(order, null);
    }

    public OrderResponse toOrderResponse(Order order, CreatePaymentResponse paymentResponse) {
        List<SubOrder> sourceSubOrders = Optional.ofNullable(order.getSubOrders()).orElseGet(List::of);
        Map<Long, List<ReturnRequestResponse>> returnsBySubOrder = loadReturnsBySubOrderIds(
            sourceSubOrders.stream().map(SubOrder::getId).toList()
        );
        List<SubOrderResponse> subOrders = sourceSubOrders.stream()
            .map(so -> toSubOrderResponse(so, returnsBySubOrder.getOrDefault(so.getId(), List.of())))
            .toList();

        return OrderResponse.builder()
            .id(order.getId())
            .paymentId(paymentResponse != null ? paymentResponse.getPaymentId() : null)
            .orderCode(order.getOrderCode())
            .customerKeycloakId(order.getCustomerKeycloakId())
            .platformDiscountCode(order.getPlatformDiscountCode())
            .platformDiscountAmount(order.getPlatformDiscountAmount())
            .platformDiscountHoldId(order.getPlatformDiscountHoldId())
            .totalPayableAmount(order.getTotalPayableAmount())
            .paymentStatus(order.getPaymentStatus())
            .paymentMethod(order.getPaymentMethod())
            .checkoutUrl(paymentResponse != null ? paymentResponse.getCheckoutUrl() : null)
            .shippingAddress(order.getShippingAddress())
            .customerName(order.getCustomerName())
            .customerPhone(order.getCustomerPhone())
            .note(order.getNote())
            .createdAt(order.getCreatedAt())
            .updatedAt(order.getUpdatedAt())
            .subOrders(subOrders)
            .build();
    }

    public SubOrderResponse toSubOrderResponse(SubOrder so) {
        List<ReturnRequestResponse> returns = so.getId() == null
            ? List.of()
            : loadReturnsBySubOrderIds(List.of(so.getId())).getOrDefault(so.getId(), List.of());
        return toSubOrderResponse(so, returns);
    }

    private SubOrderResponse toSubOrderResponse(SubOrder so, List<ReturnRequestResponse> returns) {
        List<OrderItemResponse> items = Optional.ofNullable(so.getItems()).orElseGet(List::of).stream()
            .map(this::toOrderItemResponse)
            .toList();

        Order order = so.getOrder();
        return SubOrderResponse.builder()
            .id(so.getId())
            .vendorKeycloakId(so.getVendorKeycloakId())
            .status(so.getStatus())
            .payoutStatus(so.getPayoutStatus())
            .payoutRef(so.getPayoutRef())
            .payoutAt(so.getPayoutAt())
            .completedAt(so.getCompletedAt())
            .returns(returns)
            .subtotal(so.getSubtotal())
            .finalAmount(so.getFinalAmount())
            .items(items)
            .orderCode(parent(order, Order::getOrderCode))
            .createdAt(parent(order, Order::getCreatedAt))
            .customerKeycloakId(parent(order, Order::getCustomerKeycloakId))
            .customerName(parent(order, Order::getCustomerName))
            .customerPhone(parent(order, Order::getCustomerPhone))
            .shippingAddress(parent(order, Order::getShippingAddress))
            .note(parent(order, Order::getNote))
            .paymentStatus(parent(order, Order::getPaymentStatus))
            .paymentMethod(parent(order, Order::getPaymentMethod))
            .build();
    }

    public SubOrderSummaryResponse toSubOrderSummaryResponse(SubOrder so) {
        Order order = so.getOrder();
        return SubOrderSummaryResponse.builder()
            .id(so.getId())
            .status(so.getStatus())
            .payoutStatus(so.getPayoutStatus())
            .subtotal(so.getSubtotal())
            .finalAmount(so.getFinalAmount())
            .orderCode(parent(order, Order::getOrderCode))
            .createdAt(parent(order, Order::getCreatedAt))
            .customerKeycloakId(parent(order, Order::getCustomerKeycloakId))
            .customerName(parent(order, Order::getCustomerName))
            .customerPhone(parent(order, Order::getCustomerPhone))
            .shippingAddress(parent(order, Order::getShippingAddress))
            .note(parent(order, Order::getNote))
            .paymentStatus(parent(order, Order::getPaymentStatus))
            .paymentMethod(parent(order, Order::getPaymentMethod))
            .build();
    }

    private static <T> T parent(Order order, Function<Order, T> getter) {
        return order == null ? null : getter.apply(order);
    }

    private Map<Long, List<ReturnRequestResponse>> loadReturnsBySubOrderIds(List<Long> subOrderIds) {
        List<Long> ids = Optional.ofNullable(subOrderIds).orElseGet(List::of).stream()
            .filter(id -> id != null)
            .distinct()
            .toList();
        if (ids.isEmpty()) {
            return Collections.emptyMap();
        }
        return returnRequestRepository.findBySubOrder_IdInOrderByCreatedAtDesc(ids).stream()
            .collect(Collectors.groupingBy(
                request -> request.getSubOrder().getId(),
                Collectors.mapping(returnRequestMapper::toResponse, Collectors.toList())
            ));
    }

    public OrderItemResponse toOrderItemResponse(OrderItem item) {
        return OrderItemResponse.builder()
            .id(item.getId())
            .productId(item.getProductId())
            .quantity(item.getQuantity())
            .unitPrice(item.getUnitPrice())
            .lineTotal(item.getLineTotal())
            .status(item.getStatus())
            .build();
    }
}

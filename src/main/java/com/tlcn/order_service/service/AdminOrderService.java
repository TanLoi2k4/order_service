package com.tlcn.order_service.service;

import com.tlcn.order_service.dto.AdminOrderDetailResponse;
import com.tlcn.order_service.dto.AdminOrderStatsResponse;
import com.tlcn.order_service.dto.AdminOrderSummaryResponse;
import com.tlcn.order_service.dto.OrderResponse;
import com.tlcn.order_service.dto.PageResponse;
import com.tlcn.order_service.enums.PaymentStatus;
import com.tlcn.order_service.enums.StatsPeriod;
import com.tlcn.order_service.exception.ResourceNotFoundException;
import com.tlcn.order_service.model.Order;
import com.tlcn.order_service.model.OrderItem;
import com.tlcn.order_service.model.Shipment;
import com.tlcn.order_service.model.SubOrder;
import com.tlcn.order_service.repository.OrderRepository;
import com.tlcn.order_service.repository.ShipmentRepository;
import jakarta.persistence.criteria.Predicate;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class AdminOrderService {

    private final OrderRepository orderRepository;
    private final ShipmentRepository shipmentRepository;
    private final OrderResponseMapper orderResponseMapper;

    @Transactional(readOnly = true)
    public PageResponse<AdminOrderSummaryResponse> listOrders(
        int page,
        int size,
        String search,
        String fulfillmentStatus,
        PaymentStatus paymentStatus
    ) {
        int safePage = Math.max(0, page);
        int safeSize = Math.min(Math.max(1, size), 50);
        String normalizedStatus = fulfillmentStatus != null ? fulfillmentStatus.trim().toLowerCase(Locale.ROOT) : null;

        if (normalizedStatus != null && !normalizedStatus.isBlank()) {
            List<Order> allMatching = orderRepository.findAll(buildSpecification(search, null, paymentStatus));
            List<AdminOrderSummaryResponse> filtered = allMatching.stream()
                .filter(order -> AdminOrderStatusSupport.matchesFulfillmentFilter(order, normalizedStatus))
                .map(this::toSummary)
                .toList();

            int fromIndex = safePage * safeSize;
            int toIndex = Math.min(fromIndex + safeSize, filtered.size());
            List<AdminOrderSummaryResponse> pageItems = fromIndex >= filtered.size()
                ? List.of()
                : filtered.subList(fromIndex, toIndex);
            int totalPages = filtered.isEmpty() ? 0 : (int) Math.ceil((double) filtered.size() / safeSize);

            return PageResponse.<AdminOrderSummaryResponse>builder()
                .items(pageItems)
                .page(safePage)
                .size(safeSize)
                .totalElements(filtered.size())
                .totalPages(totalPages)
                .last(safePage >= totalPages - 1)
                .build();
        }

        Page<Order> orders = orderRepository.findAll(
            buildSpecification(search, null, paymentStatus),
            PageRequest.of(safePage, safeSize, Sort.by(Sort.Direction.DESC, "createdAt"))
        );

        List<AdminOrderSummaryResponse> items = orders.getContent().stream()
            .map(this::toSummary)
            .toList();

        return PageResponse.<AdminOrderSummaryResponse>builder()
            .items(items)
            .page(orders.getNumber())
            .size(orders.getSize())
            .totalElements(orders.getTotalElements())
            .totalPages(orders.getTotalPages())
            .last(orders.isLast())
            .build();
    }

    @Transactional(readOnly = true)
    public AdminOrderStatsResponse getStats() {
        return getStats(StatsPeriod.ALL, null);
    }

    @Transactional(readOnly = true)
    public AdminOrderStatsResponse getStats(StatsPeriod period, LocalDate anchor) {
        StatsPeriodSupport.Range range = StatsPeriodSupport.resolve(period, anchor);
        Instant from = range.from();
        Instant to = range.to();
        List<Order> orders = (from != null && to != null)
            ? orderRepository.findByCreatedAtGreaterThanEqualAndCreatedAtLessThan(from, to)
            : orderRepository.findAll();
        long pending = 0;
        long processing = 0;
        long shipped = 0;
        long delivered = 0;
        long cancelled = 0;
        long refundRequested = 0;

        for (Order order : orders) {
            switch (AdminOrderStatusSupport.deriveFulfillmentStatus(order)) {
                case "pending" -> pending++;
                case "processing" -> processing++;
                case "shipped" -> shipped++;
                case "delivered" -> delivered++;
                case "cancelled" -> cancelled++;
                case "refund_requested" -> refundRequested++;
                default -> {
                }
            }
        }

        return AdminOrderStatsResponse.builder()
            .totalOrders(orders.size())
            .pending(pending)
            .processing(processing)
            .shipped(shipped)
            .delivered(delivered)
            .cancelled(cancelled)
            .refundRequested(refundRequested)
            .build();
    }

    @Transactional(readOnly = true)
    public AdminOrderDetailResponse getOrderDetail(String orderCode) {
        Order order = orderRepository.findByOrderCode(orderCode)
            .orElseThrow(() -> new ResourceNotFoundException("Order not found"));

        OrderResponse response = orderResponseMapper.toOrderResponse(order);
        String trackingNumber = resolveTrackingNumber(order);

        return AdminOrderDetailResponse.builder()
            .order(response)
            .trackingNumber(trackingNumber)
            .build();
    }

    private Specification<Order> buildSpecification(String search, String ignored, PaymentStatus paymentStatus) {
        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (search != null && !search.isBlank()) {
                String pattern = "%" + search.trim().toLowerCase(Locale.ROOT) + "%";
                predicates.add(cb.or(
                    cb.like(cb.lower(root.get("orderCode")), pattern),
                    cb.like(cb.lower(root.get("customerName")), pattern),
                    cb.like(cb.lower(root.get("customerPhone")), pattern)
                ));
            }

            if (paymentStatus != null) {
                predicates.add(cb.equal(root.get("paymentStatus"), paymentStatus));
            }

            if (predicates.isEmpty()) {
                return cb.conjunction();
            }
            return cb.and(predicates.toArray(new Predicate[0]));
        };
    }

    private AdminOrderSummaryResponse toSummary(Order order) {
        List<SubOrder> subOrders = Optional.ofNullable(order.getSubOrders()).orElse(List.of());
        Set<String> vendorIds = new LinkedHashSet<>();
        int itemCount = 0;

        for (SubOrder subOrder : subOrders) {
            if (subOrder.getVendorKeycloakId() != null && !subOrder.getVendorKeycloakId().isBlank()) {
                vendorIds.add(subOrder.getVendorKeycloakId());
            }
            itemCount += Optional.ofNullable(subOrder.getItems()).orElse(List.of()).stream()
                .map(OrderItem::getQuantity)
                .filter(Objects::nonNull)
                .mapToInt(Integer::intValue)
                .sum();
        }

        return AdminOrderSummaryResponse.builder()
            .orderCode(order.getOrderCode())
            .customerName(order.getCustomerName())
            .customerKeycloakId(order.getCustomerKeycloakId())
            .vendorKeycloakIds(List.copyOf(vendorIds))
            .itemCount(itemCount)
            .totalPayableAmount(order.getTotalPayableAmount())
            .fulfillmentStatus(AdminOrderStatusSupport.deriveFulfillmentStatus(order))
            .paymentStatus(order.getPaymentStatus())
            .createdAt(order.getCreatedAt())
            .build();
    }

    private String resolveTrackingNumber(Order order) {
        for (SubOrder subOrder : Optional.ofNullable(order.getSubOrders()).orElse(List.of())) {
            Optional<Shipment> shipment = shipmentRepository.findBySubOrder_Id(subOrder.getId());
            if (shipment.isPresent()) {
                String externalId = shipment.get().getExternalShipmentId();
                if (externalId != null && !externalId.isBlank()) {
                    return externalId;
                }
            }
        }
        return null;
    }
}

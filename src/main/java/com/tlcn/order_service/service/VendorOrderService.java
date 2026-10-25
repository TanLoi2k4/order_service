package com.tlcn.order_service.service;

import com.tlcn.order_service.client.PaymentClient;
import com.tlcn.order_service.dto.PageResponse;
import com.tlcn.order_service.dto.ProductSalesAggregateDTO;
import com.tlcn.order_service.dto.SubOrderResponse;
import com.tlcn.order_service.dto.SubOrderSummaryResponse;
import com.tlcn.order_service.dto.VendorStatsPeriodSlice;
import com.tlcn.order_service.dto.VendorSubOrderStatsResponse;
import com.tlcn.order_service.dto.kafka.DiscountCommand;
import com.tlcn.order_service.dto.kafka.DiscountCommandType;
import com.tlcn.order_service.dto.kafka.StockRollbackRequest;
import com.tlcn.order_service.dto.kafka.SubOrderShippingRequested;
import com.tlcn.order_service.enums.OrderItemStatus;
import com.tlcn.order_service.enums.PaymentMethod;
import com.tlcn.order_service.enums.PaymentStatus;
import com.tlcn.order_service.enums.PayoutStatus;
import com.tlcn.order_service.enums.StatsPeriod;
import com.tlcn.order_service.enums.SubOrderStatus;
import com.tlcn.order_service.exception.BusinessException;
import com.tlcn.order_service.exception.ResourceNotFoundException;
import com.tlcn.order_service.kafka.DiscountCommandProducer;
import com.tlcn.order_service.kafka.OrderEventProducer;
import com.tlcn.order_service.model.Order;
import com.tlcn.order_service.model.OrderItem;
import com.tlcn.order_service.model.SubOrder;
import com.tlcn.order_service.repository.OrderItemRepository;
import com.tlcn.order_service.repository.OrderRepository;
import com.tlcn.order_service.repository.SubOrderRepository;
import com.tlcn.order_service.util.MoneyUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class VendorOrderService {

    private final SubOrderRepository subOrderRepository;
    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final PaymentClient paymentClient;

    private final OrderEventProducer orderEventProducer;
    private final DiscountCommandProducer discountCommandProducer;
    private final OrderAmountService orderAmountService;
    private final OrderResponseMapper orderResponseMapper;
    private final DiscountHoldService discountHoldService;

    @Transactional(readOnly = true)
    public PageResponse<SubOrderSummaryResponse> getMySubOrders(
        String vendorKeycloakId,
        int page,
        int size,
        SubOrderStatus status,
        String customerKeycloakId,
        String q
    ) {
        int safePage = Math.max(0, page);
        int safeSize = Math.min(Math.max(1, size), 50);

        String trimmedCustomerId = customerKeycloakId == null ? null : customerKeycloakId.trim();
        if (trimmedCustomerId != null && trimmedCustomerId.isEmpty()) {
            trimmedCustomerId = null;
        }

        String search = q == null ? null : q.trim().toLowerCase();
        if (search != null && search.isEmpty()) {
            search = null;
        }
        if (search != null) {
            search = "%" + search + "%";
        }

        Page<SubOrder> result = subOrderRepository.findVendorSubOrders(
            vendorKeycloakId,
            status,
            trimmedCustomerId,
            search,
            PageRequest.of(safePage, safeSize, Sort.by(Sort.Direction.DESC, "id"))
        );

        List<SubOrderSummaryResponse> items = result.getContent().stream()
            .map(orderResponseMapper::toSubOrderSummaryResponse)
            .toList();

        return PageResponse.<SubOrderSummaryResponse>builder()
            .items(items)
            .page(result.getNumber())
            .size(result.getSize())
            .totalElements(result.getTotalElements())
            .totalPages(result.getTotalPages())
            .last(result.isLast())
            .build();
    }

    @Transactional(readOnly = true)
    public VendorSubOrderStatsResponse getMySubOrderStats(String vendorKeycloakId) {
        return getMySubOrderStats(vendorKeycloakId, StatsPeriod.ALL, null);
    }

    @Transactional(readOnly = true)
    public VendorSubOrderStatsResponse getMySubOrderStats(
        String vendorKeycloakId,
        StatsPeriod period,
        LocalDate anchor
    ) {
        StatsPeriod safePeriod = period == null ? StatsPeriod.ALL : period;
        StatsPeriodSupport.Range currentRange = StatsPeriodSupport.resolve(safePeriod, anchor);
        VendorStatsPeriodSlice current = loadStatsSlice(vendorKeycloakId, safePeriod, currentRange);

        return VendorSubOrderStatsResponse.builder()
            .total(current.getTotal())
            .byStatus(current.getByStatus())
            .period(current)
            .build();
    }

    private VendorStatsPeriodSlice loadStatsSlice(
        String vendorKeycloakId,
        StatsPeriod period,
        StatsPeriodSupport.Range range
    ) {
        Map<SubOrderStatus, Long> byStatus = emptyStatusCounts();
        long total = 0L;
        Instant from = range == null ? null : range.from();
        Instant to = range == null ? null : range.to();
        List<Object[]> rows = (from != null && to != null)
            ? subOrderRepository.countVendorSubOrdersByStatusBetween(vendorKeycloakId, from, to)
            : subOrderRepository.countVendorSubOrdersByStatus(vendorKeycloakId);

        for (Object[] row : rows) {
            if (row == null || row.length < 2 || row[0] == null || row[1] == null) {
                continue;
            }
            SubOrderStatus status = (SubOrderStatus) row[0];
            long count = ((Number) row[1]).longValue();
            byStatus.put(status, count);
            total += count;
        }

        return VendorStatsPeriodSlice.builder()
            .granularity(period)
            .from(from)
            .to(to)
            .label(range == null ? "Tất cả thời gian" : range.label())
            .total(total)
            .byStatus(byStatus)
            .build();
    }

    private static Map<SubOrderStatus, Long> emptyStatusCounts() {
        Map<SubOrderStatus, Long> byStatus = new EnumMap<>(SubOrderStatus.class);
        for (SubOrderStatus status : SubOrderStatus.values()) {
            byStatus.put(status, 0L);
        }
        return byStatus;
    }

    @Transactional(readOnly = true)
    public List<ProductSalesAggregateDTO> getCompletedProductSales(
        String vendorKeycloakId,
        LocalDate from,
        LocalDate to,
        int limit
    ) {
        LocalDate safeFrom = from == null ? StatsPeriodSupport.today().minusDays(29) : from;
        LocalDate safeTo = to == null ? StatsPeriodSupport.today() : to;
        if (safeFrom.isAfter(safeTo)) {
            LocalDate tmp = safeFrom;
            safeFrom = safeTo;
            safeTo = tmp;
        }

        Instant fromInstant = safeFrom.atStartOfDay(StatsPeriodSupport.ZONE).toInstant();
        Instant toInstant = safeTo.plusDays(1).atStartOfDay(StatsPeriodSupport.ZONE).toInstant();
        int safeLimit = Math.min(Math.max(limit, 1), 50);

        List<Object[]> rows = orderItemRepository.aggregateCompletedProductSales(
            vendorKeycloakId,
            fromInstant,
            toInstant,
            PageRequest.of(0, safeLimit)
        );
        return mapProductSalesRows(rows);
    }

    @Transactional(readOnly = true)
    public List<ProductSalesAggregateDTO> getCompletedPlatformProductSales(
        LocalDate from,
        LocalDate to,
        int limit
    ) {
        LocalDate safeFrom = from == null ? StatsPeriodSupport.today().minusDays(29) : from;
        LocalDate safeTo = to == null ? StatsPeriodSupport.today() : to;
        if (safeFrom.isAfter(safeTo)) {
            LocalDate tmp = safeFrom;
            safeFrom = safeTo;
            safeTo = tmp;
        }

        Instant fromInstant = safeFrom.atStartOfDay(StatsPeriodSupport.ZONE).toInstant();
        Instant toInstant = safeTo.plusDays(1).atStartOfDay(StatsPeriodSupport.ZONE).toInstant();
        int safeLimit = Math.min(Math.max(limit, 1), 50);

        List<Object[]> rows = orderItemRepository.aggregateCompletedPlatformProductSales(
            fromInstant,
            toInstant,
            PageRequest.of(0, safeLimit)
        );
        return mapProductSalesRows(rows);
    }

    @Transactional(readOnly = true)
    public List<ProductSalesAggregateDTO> getLifetimeProductSold() {
        return mapProductSalesRows(orderItemRepository.aggregateLifetimeProductSold());
    }

    private List<ProductSalesAggregateDTO> mapProductSalesRows(List<Object[]> rows) {
        List<ProductSalesAggregateDTO> result = new ArrayList<>();
        for (Object[] row : rows) {
            if (row == null || row.length < 3 || row[0] == null) {
                continue;
            }
            result.add(ProductSalesAggregateDTO.builder()
                .productId(((Number) row[0]).longValue())
                .quantity(row[1] == null ? 0L : ((Number) row[1]).longValue())
                .revenue(toRevenue(row[2]))
                .build());
        }
        return result;
    }

    private static BigDecimal toRevenue(Object value) {
        if (value instanceof BigDecimal money) {
            return MoneyUtils.normalizeMoney(money);
        }
        if (value instanceof Number number) {
            return MoneyUtils.safeMoney(number.doubleValue());
        }
        return MoneyUtils.normalizeMoney(null);
    }

    @Transactional(readOnly = true)
    public SubOrderResponse getMySubOrderDetail(String vendorKeycloakId, Long subOrderId) {
        SubOrder subOrder = subOrderRepository.findByIdWithOrderAndItems(subOrderId)
            .filter(so -> vendorKeycloakId.equals(so.getVendorKeycloakId()))
            .orElseThrow(() -> new ResourceNotFoundException("SubOrder not found"));

        return orderResponseMapper.toSubOrderResponse(subOrder);
    }

    @Transactional
    public SubOrderResponse updateMySubOrderStatus(String vendorKeycloakId, Long subOrderId, SubOrderStatus newStatus, String authorizationHeader) {
        if (newStatus == null) {
            throw new BusinessException("Status is required", HttpStatus.BAD_REQUEST, "STATUS_REQUIRED");
        }

        SubOrder subOrder = subOrderRepository.findByIdAndVendorKeycloakId(subOrderId, vendorKeycloakId)
            .orElseThrow(() -> new ResourceNotFoundException("SubOrder not found"));

        // Vendor flow: confirm each item first, then move suborder to PROCESSING
        if (newStatus != SubOrderStatus.PROCESSING && newStatus != SubOrderStatus.CANCELLED) {
            throw new BusinessException("Vendor cannot set this suborder status", HttpStatus.BAD_REQUEST, "SUBORDER_STATUS_NOT_ALLOWED");
        }

        if (subOrder.getStatus() == SubOrderStatus.SHIPPING
            || subOrder.getStatus() == SubOrderStatus.SHIPPING_REQUESTED
            || subOrder.getStatus() == SubOrderStatus.RETURNING
            || subOrder.getStatus() == SubOrderStatus.COMPLETED) {
            throw new BusinessException("SubOrder cannot be updated at this stage", HttpStatus.BAD_REQUEST, "SUBORDER_LOCKED");
        }

        if (newStatus == SubOrderStatus.PROCESSING) {
            if (subOrder.getStatus() != SubOrderStatus.CREATED) {
                throw new BusinessException("SubOrder must be CREATED to start processing", HttpStatus.BAD_REQUEST, "INVALID_SUBORDER_STATE");
            }

            List<OrderItem> items = Optional.ofNullable(subOrder.getItems()).orElseGet(List::of);
            if (items.isEmpty()) {
                throw new BusinessException("SubOrder has no items", HttpStatus.BAD_REQUEST, "EMPTY_SUBORDER");
            }

            boolean anyPending = items.stream().anyMatch(i -> i.getStatus() == OrderItemStatus.PENDING);
            if (anyPending) {
                throw new BusinessException("All items must be confirmed or cancelled before confirming suborder", HttpStatus.BAD_REQUEST, "ITEMS_NOT_CONFIRMED");
            }

            boolean anyConfirmed = items.stream().anyMatch(i -> i.getStatus() == OrderItemStatus.CONFIRMED);
            if (!anyConfirmed) {
                throw new BusinessException("At least one item must be confirmed to process suborder", HttpStatus.BAD_REQUEST, "NO_CONFIRMED_ITEMS");
            }
        }

        if (newStatus == SubOrderStatus.CANCELLED) {
            List<OrderItem> items = Optional.ofNullable(subOrder.getItems()).orElseGet(List::of);
            for (OrderItem item : items) {
                if (item == null) {
                    continue;
                }

                OrderItemStatus previous = item.getStatus();

                if (previous == OrderItemStatus.RETURNED || previous == OrderItemStatus.CANCELLED) {
                    continue;
                }

                item.setStatus(OrderItemStatus.CANCELLED);

                // Rollback reserved stock once (on first transition to CANCELLED)
                String orderCode = subOrder.getOrder() == null ? null : subOrder.getOrder().getOrderCode();
                if (orderCode != null && item.getProductId() != null && item.getQuantity() != null) {
                    StockRollbackRequest rollback = StockRollbackRequest.builder()
                        .orderId(orderCode)
                        .productId(item.getProductId())
                        .quantity(item.getQuantity())
                        .timestamp(LocalDateTime.now())
                        .build();
                    orderEventProducer.sendStockRollbackRequest(rollback);
                }
            }

            subOrder.setPayoutStatus(PayoutStatus.CANCELLED);
        }

        subOrder.setStatus(newStatus);
        if (newStatus == SubOrderStatus.CANCELLED) {
            syncOrderAfterVendorCancellation(subOrder, vendorKeycloakId, authorizationHeader);
        }
        SubOrder saved = subOrderRepository.save(subOrder);
        return orderResponseMapper.toSubOrderResponse(saved);
    }

    @Transactional
    public SubOrderResponse updateMyOrderItemStatus(
        String vendorKeycloakId,
        Long subOrderId,
        Long itemId,
        OrderItemStatus newStatus,
        String authorizationHeader
    ) {
        if (newStatus == null) {
            throw new BusinessException("Order item status is required", HttpStatus.BAD_REQUEST, "ORDER_ITEM_STATUS_REQUIRED");
        }

        SubOrder subOrder = subOrderRepository.findByIdAndVendorKeycloakId(subOrderId, vendorKeycloakId)
            .orElseThrow(() -> new ResourceNotFoundException("SubOrder not found"));

        if (subOrder.getStatus() != SubOrderStatus.CREATED) {
            throw new BusinessException("Order items can only be confirmed while suborder is CREATED", HttpStatus.BAD_REQUEST, "SUBORDER_LOCKED");
        }

        if (newStatus != OrderItemStatus.CONFIRMED && newStatus != OrderItemStatus.CANCELLED) {
            throw new BusinessException("Vendor can only CONFIRM or CANCEL an item", HttpStatus.BAD_REQUEST, "ITEM_STATUS_NOT_ALLOWED");
        }

        OrderItem item = Optional.ofNullable(subOrder.getItems()).orElseGet(List::of)
            .stream()
            .filter(i -> i.getId() != null && i.getId().equals(itemId))
            .findFirst()
            .orElseThrow(() -> new ResourceNotFoundException("Order item not found"));

        if (item.getStatus() != OrderItemStatus.PENDING) {
            throw new BusinessException("Only PENDING items can be updated", HttpStatus.BAD_REQUEST, "ORDER_ITEM_LOCKED");
        }

        item.setStatus(newStatus);

        if (newStatus == OrderItemStatus.CANCELLED) {
            String orderCode = subOrder.getOrder() == null ? null : subOrder.getOrder().getOrderCode();
            if (orderCode != null && item.getProductId() != null && item.getQuantity() != null) {
                StockRollbackRequest rollback = StockRollbackRequest.builder()
                    .orderId(orderCode)
                    .productId(item.getProductId())
                    .quantity(item.getQuantity())
                    .timestamp(LocalDateTime.now())
                    .build();
                orderEventProducer.sendStockRollbackRequest(rollback);
            }

            List<OrderItem> items = Optional.ofNullable(subOrder.getItems()).orElseGet(List::of);
            boolean allCancelled = !items.isEmpty() && items.stream().allMatch(i -> i.getStatus() == OrderItemStatus.CANCELLED);
            if (allCancelled) {
                subOrder.setStatus(SubOrderStatus.CANCELLED);
                subOrder.setPayoutStatus(PayoutStatus.CANCELLED);
            }

            syncOrderAfterVendorCancellation(subOrder, vendorKeycloakId, authorizationHeader);
        }

        SubOrder saved = subOrderRepository.save(subOrder);
        return orderResponseMapper.toSubOrderResponse(saved);
    }

    @Transactional
    public SubOrderResponse requestShipment(String vendorKeycloakId, Long subOrderId, String serviceLevel, String carrierPreference, String pickupAddress) {
        SubOrder subOrder = subOrderRepository.findByIdAndVendorKeycloakId(subOrderId, vendorKeycloakId)
            .orElseThrow(() -> new ResourceNotFoundException("SubOrder not found"));

        if (subOrder.getStatus() != SubOrderStatus.PROCESSING) {
            throw new BusinessException("SubOrder must be PROCESSING before requesting shipment", HttpStatus.BAD_REQUEST, "SUBORDER_NOT_READY_FOR_SHIPPING");
        }

        if (subOrder.getOrder() == null) {
            throw new BusinessException("SubOrder missing order information", HttpStatus.BAD_REQUEST, "ORDER_NOT_FOUND");
        }

        if (subOrder.getOrder().getPaymentMethod() != PaymentMethod.COD
            && subOrder.getOrder().getPaymentStatus() != PaymentStatus.COMPLETED) {
            throw new BusinessException("Không thể yêu cầu giao hàng trước khi thanh toán hoàn tất với phương thức trả trước", HttpStatus.BAD_REQUEST, "PAYMENT_NOT_COMPLETED");
        }

        List<OrderItem> confirmedItems = Optional.ofNullable(subOrder.getItems()).orElseGet(List::of).stream()
            .filter(Objects::nonNull)
            .filter(i -> i.getStatus() == OrderItemStatus.CONFIRMED)
            .toList();

        if (confirmedItems.isEmpty()) {
            throw new BusinessException("SubOrder has no confirmed items to ship", HttpStatus.BAD_REQUEST, "NO_CONFIRMED_ITEMS_TO_SHIP");
        }

        String shipmentServiceLevel = (serviceLevel == null || serviceLevel.isBlank()) ? "STANDARD" : serviceLevel.trim().toUpperCase();
        String preferredCarrier = (carrierPreference == null || carrierPreference.isBlank()) ? null : carrierPreference.trim().toUpperCase();
        String correlationId = UUID.randomUUID().toString();

        subOrder.setStatus(SubOrderStatus.SHIPPING_REQUESTED);
        SubOrder saved = subOrderRepository.save(subOrder);

        SubOrderShippingRequested request = SubOrderShippingRequested.builder()
            .eventId(UUID.randomUUID().toString())
            .correlationId(correlationId)
            .subOrderId(saved.getId())
            .orderId(saved.getOrder().getId())
            .vendorKeycloakId(saved.getVendorKeycloakId())
            .pickupAddress(pickupAddress)
            .deliveryAddress(saved.getOrder().getShippingAddress())
            .serviceLevel(shipmentServiceLevel)
            .carrierPreference(preferredCarrier)
            .createdAt(Instant.now())
            .build();

        orderEventProducer.sendSubOrderShippingRequested(request);
        return orderResponseMapper.toSubOrderResponse(saved);
    }

    private void syncOrderAfterVendorCancellation(SubOrder subOrder, String vendorKeycloakId, String authorizationHeader) {
        Order order = subOrder.getOrder();
        if (order == null) {
            return;
        }

        PaymentStatus originalPaymentStatus = order.getPaymentStatus();
        BigDecimal originalTotalPayable = MoneyUtils.normalizeMoney(order.getTotalPayableAmount());

        boolean nowFullyCancelled = Optional.ofNullable(order.getSubOrders()).orElseGet(List::of)
            .stream()
            .allMatch(so -> so.getStatus() == SubOrderStatus.CANCELLED);

        BigDecimal recalculatedDiscount = Optional.ofNullable(order.getPlatformDiscountAmount()).orElse(BigDecimal.ZERO);
        Long holdIdToReleaseBecauseIneligible = null;

        if (nowFullyCancelled) {
            recalculatedDiscount = BigDecimal.ZERO;
        } else if (order.getPlatformDiscountHoldId() != null
            && order.getPlatformDiscountCode() != null
            && !order.getPlatformDiscountCode().isBlank()) {

            double remainingSubtotal = orderAmountService.sumActiveItemSubtotal(order);
            DiscountHoldService.DiscountRecalculationResult result = discountHoldService.recalculateHold(
                vendorKeycloakId,
                order,
                remainingSubtotal,
                "VENDOR_ORDER_ITEM_CANCELLED_RECALCULATE"
            );

            recalculatedDiscount = result.discountAmount();
            if (result.holdStillValid()) {
                order.setPlatformDiscountHoldId(result.holdId());
            } else {
                holdIdToReleaseBecauseIneligible = order.getPlatformDiscountHoldId();
                order.setPlatformDiscountHoldId(null);
                order.setPlatformDiscountCode(null);
            }
        }

        orderAmountService.recomputeOrderAmountsFromItemStates(order, recalculatedDiscount);

        syncPaymentAfterVendorCancel(
            order,
            originalPaymentStatus,
            originalTotalPayable,
            authorizationHeader,
            nowFullyCancelled
        );

        if (nowFullyCancelled
            && (order.getPaymentStatus() == PaymentStatus.COMPLETED || order.getPaymentStatus() == PaymentStatus.PARTIALLY_REFUNDED)) {
            order.setPaymentStatus(PaymentStatus.REFUNDED);
        } else if (nowFullyCancelled && order.getPaymentStatus() == PaymentStatus.PENDING) {
            order.setPaymentStatus(PaymentStatus.CANCELLED);
        } else if (!nowFullyCancelled && order.getPaymentStatus() == PaymentStatus.COMPLETED) {
            order.setPaymentStatus(PaymentStatus.PARTIALLY_REFUNDED);
        }

        Order savedOrder = orderRepository.save(order);

        if (holdIdToReleaseBecauseIneligible != null) {
            DiscountCommand releaseCmd = DiscountCommand.builder()
                .type(DiscountCommandType.RELEASE_REQUESTED)
                .orderId(savedOrder.getId())
                .userId(vendorKeycloakId)
                .holdId(holdIdToReleaseBecauseIneligible)
                .createdAt(Instant.now())
                .reason("VENDOR_ORDER_ITEM_CANCELLED_DISCOUNT_INELIGIBLE")
                .build();

            discountCommandProducer.send(releaseCmd);
        }

        if (nowFullyCancelled
            && savedOrder.getPlatformDiscountHoldId() != null
            && savedOrder.getPlatformDiscountCode() != null
            && !savedOrder.getPlatformDiscountCode().isBlank()) {

            DiscountCommand releaseCmd = DiscountCommand.builder()
                .type(DiscountCommandType.RELEASE_REQUESTED)
                .orderId(savedOrder.getId())
                .userId(vendorKeycloakId)
                .code(savedOrder.getPlatformDiscountCode())
                .holdId(savedOrder.getPlatformDiscountHoldId())
                .createdAt(Instant.now())
                .reason("VENDOR_ORDER_CANCELLED")
                .build();

            discountCommandProducer.send(releaseCmd);
        }
    }

    private void syncPaymentAfterVendorCancel(
        Order order,
        PaymentStatus originalPaymentStatus,
        BigDecimal originalTotalPayable,
        String authorizationHeader,
        boolean nowFullyCancelled
    ) {
        BigDecimal currentTotalPayable = MoneyUtils.normalizeMoney(order.getTotalPayableAmount());

        if (originalPaymentStatus == PaymentStatus.PENDING) {
            if (nowFullyCancelled || currentTotalPayable.compareTo(BigDecimal.ZERO) <= 0) {
                // Use canonical orderCode as idempotency key so payment-service can match the original create
                cancelPendingPaymentInPaymentService(order.getOrderCode(), authorizationHeader, order.getOrderCode());
                order.setPaymentStatus(PaymentStatus.CANCELLED);
                return;
            }

            if (currentTotalPayable.compareTo(originalTotalPayable) != 0) {
                updatePendingPaymentInPaymentService(order.getOrderCode(), authorizationHeader, currentTotalPayable);
            }
            return;
        }

        if (originalPaymentStatus == PaymentStatus.COMPLETED || originalPaymentStatus == PaymentStatus.PARTIALLY_REFUNDED) {
            BigDecimal refundAmount = MoneyUtils.normalizeMoney(originalTotalPayable.subtract(currentTotalPayable));
            if (refundAmount.compareTo(BigDecimal.ZERO) > 0) {
                // Unique per refund attempt so partial refunds on the same order do not collide
                refundPaymentInPaymentService(
                    order.getOrderCode(),
                    authorizationHeader,
                    refundAmount,
                    nowFullyCancelled ? "VENDOR_ORDER_CANCELLED" : "VENDOR_ORDER_ITEM_CANCELLED",
                    buildRefundIdempotencyKey(order.getOrderCode(), refundAmount, currentTotalPayable)
                );
            }
        }
    }

    private String buildRefundIdempotencyKey(String orderCode, BigDecimal refundAmount, BigDecimal remainingPayable) {
        return "refund:" + orderCode + ":" + refundAmount.toPlainString() + ":" + remainingPayable.toPlainString();
    }

    private void updatePendingPaymentInPaymentService(String orderCode, String authorizationHeader, BigDecimal amount) {
        paymentClient.updateVendorPayment(orderCode, authorizationHeader, amount);
    }

    private void cancelPendingPaymentInPaymentService(String orderCode, String authorizationHeader, String idempotencyKey) {
        paymentClient.cancelVendorPayment(orderCode, authorizationHeader, idempotencyKey);
    }

    private void refundPaymentInPaymentService(
        String orderCode,
        String authorizationHeader,
        BigDecimal amount,
        String reason,
        String idempotencyKey
    ) {
        paymentClient.refundVendorPayment(orderCode, authorizationHeader, amount, reason, idempotencyKey);
    }
}

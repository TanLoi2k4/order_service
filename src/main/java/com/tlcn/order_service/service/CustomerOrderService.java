package com.tlcn.order_service.service;

import com.tlcn.order_service.dto.CoPurchaseCountDto;
import com.tlcn.order_service.dto.CreateOrderRequest;
import com.tlcn.order_service.client.PaymentClient;
import com.tlcn.order_service.dto.CreatePaymentResponse;
import com.tlcn.order_service.dto.OrderResponse;
import com.tlcn.order_service.dto.OrderSummaryResponse;
import com.tlcn.order_service.dto.PageResponse;
import com.tlcn.order_service.dto.ReviewEligibilityResponse;
import com.tlcn.order_service.dto.UpdatePaymentRequest;
import com.tlcn.order_service.dto.kafka.DiscountCommand;
import com.tlcn.order_service.dto.kafka.DiscountCommandType;
import com.tlcn.order_service.dto.kafka.StockReservationRequest;
import com.tlcn.order_service.dto.kafka.StockRollbackRequest;
import com.tlcn.order_service.enums.OrderItemStatus;
import com.tlcn.order_service.enums.SubOrderStatus;
import com.tlcn.order_service.enums.PaymentStatus;
import com.tlcn.order_service.enums.PayoutStatus;
import com.tlcn.order_service.exception.BusinessException;
import com.tlcn.order_service.exception.ResourceNotFoundException;
import com.tlcn.order_service.kafka.DiscountCommandProducer;
import com.tlcn.order_service.kafka.OrderEventProducer;
import com.tlcn.order_service.model.*;
import com.tlcn.order_service.repository.OrderItemRepository;
import com.tlcn.order_service.repository.OrderRepository;
import com.tlcn.order_service.util.MoneyUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeoutException;

@Service
@RequiredArgsConstructor
@Slf4j
public class CustomerOrderService {

    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final PaymentClient paymentClient;

    private final OrderEventProducer orderEventProducer;
    private final DiscountCommandProducer discountCommandProducer;
    private final OrderCheckoutWriter orderCheckoutWriter;
    private final OrderCompensationService orderCompensationService;
    private final DiscountReplyAwaiter discountReplyAwaiter;
    private final OrderAmountService orderAmountService;
    private final OrderResponseMapper orderResponseMapper;
    private final DiscountHoldService discountHoldService;
    private final StockReservationAwaiter stockReservationAwaiter;

    public OrderResponse createOrderFromCart(String customerKeycloakId, CreateOrderRequest request, String authorizationHeader) {
        OrderCheckoutWriter.CartSnapshot snapshot = orderCheckoutWriter.loadCartSnapshot(customerKeycloakId);

        OrderCheckoutWriter.DiscountHoldData holdReply = null;
        String code = request != null ? request.getPlatformDiscountCode() : null;
        if (code != null && !code.isBlank()) {
            holdReply = requestDiscountHoldOrThrow(customerKeycloakId, code, snapshot.subtotal());
        }

        Order saved;
        try {
            // Phase 1: Save order + clear cart (separate bean so @Transactional applies)
            saved = orderCheckoutWriter.createOrderFromCart(
                customerKeycloakId,
                request,
                holdReply,
                snapshot.version()
            );
        } catch (RuntimeException ex) {
            // Hold succeeded but DB failed → release orphan hold
            if (holdReply != null) {
                orderCompensationService.releaseOrphanDiscountHold(
                    customerKeycloakId,
                    code,
                    holdReply.holdId(),
                    "ORDER_PERSIST_FAILED"
                );
            }
            throw ex;
        }

        // Phase 2: Hold stock at place-order (before payment) so COD and unpaid VNPAY also reserve
        reserveStockAtCheckoutOrThrow(saved);

        // Phase 3: Create payment (outside transaction, long HTTP call)
        CreatePaymentResponse paymentResponse;
        try {
            paymentResponse = createPaymentForOrder(saved, authorizationHeader);
        } catch (BusinessException e) {
            log.error("Failed to create payment for orderCode={}: {}", saved.getOrderCode(), e.getMessage(), e);
            orderCompensationService.compensatePaymentCreateFailure(saved, "PAYMENT_CREATE_FAILED");
            throw e;
        } catch (Exception e) {
            log.error("Failed to create payment for orderCode={}: {}", saved.getOrderCode(), e.getMessage(), e);
            orderCompensationService.compensatePaymentCreateFailure(saved, "PAYMENT_CREATE_FAILED");
            throw new BusinessException("Failed to create payment", HttpStatus.BAD_GATEWAY, "PAYMENT_CREATE_FAILED");
        }

        return orderResponseMapper.toOrderResponse(saved, paymentResponse);
    }

    protected OrderCheckoutWriter.DiscountHoldData requestDiscountHoldOrThrow(String customerKeycloakId, String code, double subtotal) {
        String correlationId = UUID.randomUUID().toString();

        DiscountCommand cmd = DiscountCommand.builder()
            .type(DiscountCommandType.HOLD_REQUESTED)
            .commandId(correlationId)
            .orderId(null) // pre-order hold
            .userId(customerKeycloakId)
            .code(code)
            .subtotal(subtotal)
            .createdAt(Instant.now())
            .reason(null)
            .build();

        discountReplyAwaiter.register(correlationId);
        discountCommandProducer.send(cmd, discountCommandProducer.getDiscountRepliesTopic());

        try {
            com.tlcn.order_service.dto.kafka.DiscountEvent event = discountReplyAwaiter.await(correlationId);

            if (event == null || event.getType() == null) {
                throw new BusinessException("Discount service returned an invalid response", HttpStatus.SERVICE_UNAVAILABLE, "DISCOUNT_INVALID_REPLY");
            }

            return switch (event.getType()) {
                case HELD -> new OrderCheckoutWriter.DiscountHoldData(event.getHoldId(), MoneyUtils.safeMoney(event.getDiscountAmount()));
                case HOLD_FAILED -> throw new BusinessException("Invalid discount code", HttpStatus.BAD_REQUEST, "INVALID_DISCOUNT_CODE");
                default -> throw new BusinessException("Discount service returned an unexpected response", HttpStatus.SERVICE_UNAVAILABLE, "DISCOUNT_UNEXPECTED_REPLY");
            };
        } catch (TimeoutException ex) {
            discountReplyAwaiter.cancel(correlationId);
            throw new BusinessException("Discount service is unavailable", HttpStatus.SERVICE_UNAVAILABLE, "DISCOUNT_TIMEOUT");
        } catch (BusinessException ex) {
            discountReplyAwaiter.cancel(correlationId);
            throw ex;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            discountReplyAwaiter.cancel(correlationId);
            throw new BusinessException("Discount service is unavailable", HttpStatus.SERVICE_UNAVAILABLE, "DISCOUNT_UNAVAILABLE");
        } catch (Exception ex) {
            discountReplyAwaiter.cancel(correlationId);
            throw new BusinessException("Discount service is unavailable", HttpStatus.SERVICE_UNAVAILABLE, "DISCOUNT_UNAVAILABLE");
        }
    }

    private CreatePaymentResponse createPaymentForOrder(Order order, String authorizationHeader) {
        return paymentClient.createPaymentForOrder(order, authorizationHeader);
    }

    @Transactional(readOnly = true)
    public boolean verifyPurchase(String customerKeycloakId, Long productId) {
        if (customerKeycloakId == null || customerKeycloakId.isBlank() || productId == null) {
            return false;
        }
        return orderItemRepository.existsPurchasedProductByCustomer(customerKeycloakId, productId);
    }

    @Transactional(readOnly = true)
    public List<Long> getPurchasedProductIds(String customerKeycloakId) {
        if (customerKeycloakId == null || customerKeycloakId.isBlank()) {
            return List.of();
        }
        return orderItemRepository.findPurchasedProductIdsByCustomer(customerKeycloakId);
    }

    @Transactional(readOnly = true)
    public List<CoPurchaseCountDto> getCoPurchases(Collection<Long> sourceProductIds) {
        if (sourceProductIds == null || sourceProductIds.isEmpty()) {
            return List.of();
        }
        List<Long> ids = sourceProductIds.stream().filter(id -> id != null).distinct().toList();
        if (ids.isEmpty()) {
            return List.of();
        }
        return orderItemRepository.countCustomersWhoBoughtThen(ids).stream()
                .map(row -> CoPurchaseCountDto.builder()
                        .sourceProductId(((Number) row[0]).longValue())
                        .targetProductId(((Number) row[1]).longValue())
                        .customerCount(((Number) row[2]).longValue())
                        .build())
                .toList();
    }

    @Transactional(readOnly = true)
    public ReviewEligibilityResponse getReviewEligibility(String customerKeycloakId, Long productId) {
        if (customerKeycloakId == null || customerKeycloakId.isBlank() || productId == null) {
            return ReviewEligibilityResponse.builder().purchased(false).orderId(null).build();
        }
        List<Long> orderIds = orderItemRepository.findPurchasedOrderIdsByCustomer(
            customerKeycloakId,
            productId
        );
        Long orderId = orderIds.isEmpty() ? null : orderIds.get(0);
        return ReviewEligibilityResponse.builder()
            .purchased(orderId != null)
            .orderId(orderId)
            .build();
    }

    private void syncPaymentAfterCustomerCancel(
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
                cancelPendingPaymentInPaymentService(
                    order.getOrderCode(),
                    authorizationHeader,
                    order.getOrderCode()
                );
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
                    nowFullyCancelled ? "ORDER_CANCELLED" : "ORDER_ITEM_CANCELLED",
                    buildRefundIdempotencyKey(order.getOrderCode(), refundAmount, currentTotalPayable)
                );

                if (nowFullyCancelled) {
                    order.setPaymentStatus(PaymentStatus.REFUNDED);
                } else {
                    order.setPaymentStatus(PaymentStatus.PARTIALLY_REFUNDED);
                }
            }
        }
    }

    private String buildRefundIdempotencyKey(String orderCode, BigDecimal refundAmount, BigDecimal remainingPayable) {
        return "refund:" + orderCode + ":" + refundAmount.toPlainString() + ":" + remainingPayable.toPlainString();
    }

    private void updatePendingPaymentInPaymentService(String orderCode, String authorizationHeader, BigDecimal amount) {
        UpdatePaymentRequest request = new UpdatePaymentRequest();
        request.setAmount(amount);
        request.setCurrency("VND");
        paymentClient.updatePendingPayment(orderCode, authorizationHeader, request, orderCode);
    }

    private void cancelPendingPaymentInPaymentService(String orderCode, String authorizationHeader, String idempotencyKey) {
        paymentClient.cancelPendingPayment(orderCode, authorizationHeader, idempotencyKey);
    }

    private void refundPaymentInPaymentService(
        String orderCode,
        String authorizationHeader,
        BigDecimal amount,
        String reason,
        String idempotencyKey
    ) {
        paymentClient.refundPayment(orderCode, authorizationHeader, amount, reason, idempotencyKey);
    }

    @Transactional(readOnly = true)
    public PageResponse<OrderSummaryResponse> getMyOrders(String customerKeycloakId, int page, int size) {
        int safePage = Math.max(0, page);
        int safeSize = Math.min(Math.max(1, size), 50);

        Page<Order> orders = orderRepository.findByCustomerKeycloakId(
            customerKeycloakId,
            PageRequest.of(safePage, safeSize, Sort.by(Sort.Direction.DESC, "createdAt"))
        );

        List<OrderSummaryResponse> items = orders.getContent().stream()
            .map(o -> OrderSummaryResponse.builder()
                .orderCode(o.getOrderCode())
                .totalPayableAmount(o.getTotalPayableAmount())
                .paymentStatus(o.getPaymentStatus())
                .createdAt(o.getCreatedAt())
                .build())
            .toList();

        return PageResponse.<OrderSummaryResponse>builder()
            .items(items)
            .page(orders.getNumber())
            .size(orders.getSize())
            .totalElements(orders.getTotalElements())
            .totalPages(orders.getTotalPages())
            .last(orders.isLast())
            .build();
    }

    @Transactional(readOnly = true)
    public OrderResponse getMyOrderDetail(String customerKeycloakId, String orderCode) {
        Order order = orderRepository.findByOrderCodeAndCustomerKeycloakId(orderCode, customerKeycloakId)
            .orElseThrow(() -> new ResourceNotFoundException("Order not found"));
        return orderResponseMapper.toOrderResponse(order);
    }

    @Transactional
    public OrderResponse cancelMyOrder(String customerKeycloakId, String orderCode, String authorizationHeader) {
        Order order = orderRepository.findByOrderCodeAndCustomerKeycloakId(orderCode, customerKeycloakId)
            .orElseThrow(() -> new ResourceNotFoundException("Order not found"));

        if (order.getSubOrders() == null || order.getSubOrders().isEmpty()) {
            throw new BusinessException("Order has no suborders", HttpStatus.BAD_REQUEST, "EMPTY_ORDER");
        }

        boolean alreadyFullyCancelled = order.getSubOrders().stream().allMatch(so -> so.getStatus() == SubOrderStatus.CANCELLED);
        if (alreadyFullyCancelled) {
            return orderResponseMapper.toOrderResponse(order);
        }

        boolean anySubOrderNotCreated = order.getSubOrders().stream().anyMatch(so -> so.getStatus() != SubOrderStatus.CREATED);
        if (anySubOrderNotCreated) {
            throw new BusinessException("Order can only be cancelled before vendor starts processing", HttpStatus.BAD_REQUEST, "CANNOT_CANCEL");
        }

        boolean anyItemAlreadyHandled = order.getSubOrders().stream()
            .flatMap(so -> Optional.ofNullable(so.getItems()).orElseGet(List::of).stream())
            .anyMatch(i -> i.getStatus() != OrderItemStatus.PENDING);
        if (anyItemAlreadyHandled) {
            throw new BusinessException("Order cannot be cancelled after vendor confirmed/cancelled items", HttpStatus.BAD_REQUEST, "CANNOT_CANCEL");
        }

        PaymentStatus originalPaymentStatus = order.getPaymentStatus();
        BigDecimal originalTotalPayable = MoneyUtils.normalizeMoney(order.getTotalPayableAmount());

        for (SubOrder subOrder : order.getSubOrders()) {
            subOrder.setStatus(SubOrderStatus.CANCELLED);
            subOrder.setPayoutStatus(PayoutStatus.CANCELLED);

            for (OrderItem item : subOrder.getItems()) {
                item.setStatus(OrderItemStatus.CANCELLED);
                // Rollback stock for each item
                StockRollbackRequest rollback = StockRollbackRequest.builder()
                    .orderId(order.getOrderCode())
                    .productId(item.getProductId())
                    .quantity(item.getQuantity())
                    .timestamp(LocalDateTime.now())
                    .build();
                orderEventProducer.sendStockRollbackRequest(rollback);
            }
        }

        syncPaymentAfterCustomerCancel(
            order,
            originalPaymentStatus,
            originalTotalPayable,
            authorizationHeader,
            true
        );

        Order saved = orderRepository.save(order);

        // Release discount hold if present
        if (saved.getPlatformDiscountHoldId() != null
            && saved.getPlatformDiscountCode() != null
            && !saved.getPlatformDiscountCode().isBlank()) {

            DiscountCommand releaseCmd = DiscountCommand.builder()
                .type(DiscountCommandType.RELEASE_REQUESTED)
                .orderId(saved.getId())
                .userId(customerKeycloakId)
                .code(saved.getPlatformDiscountCode())
                .holdId(saved.getPlatformDiscountHoldId())
                .createdAt(Instant.now())
                .reason("ORDER_CANCELLED")
                .build();

            discountCommandProducer.send(releaseCmd);
        }

        return orderResponseMapper.toOrderResponse(saved);
    }

    @Transactional
    public OrderResponse cancelMyOrderItem(String customerKeycloakId, String orderCode, Long itemId, String authorizationHeader) {
        if (itemId == null) {
            throw new BusinessException("ItemId is required", HttpStatus.BAD_REQUEST, "ITEM_ID_REQUIRED");
        }

        Order order = orderRepository.findByOrderCodeAndCustomerKeycloakId(orderCode, customerKeycloakId)
            .orElseThrow(() -> new ResourceNotFoundException("Order not found"));

        if (order.getSubOrders() == null || order.getSubOrders().isEmpty()) {
            throw new BusinessException("Order has no suborders", HttpStatus.BAD_REQUEST, "EMPTY_ORDER");
        }

        boolean alreadyFullyCancelled = order.getSubOrders().stream().allMatch(so -> so.getStatus() == SubOrderStatus.CANCELLED);
        if (alreadyFullyCancelled) {
            return orderResponseMapper.toOrderResponse(order);
        }

        boolean anySubOrderAlreadyProcessing = order.getSubOrders().stream().anyMatch(so ->
            so.getStatus() == SubOrderStatus.PROCESSING
                || so.getStatus() == SubOrderStatus.SHIPPING_REQUESTED
                || so.getStatus() == SubOrderStatus.SHIPPING
                || so.getStatus() == SubOrderStatus.RETURNING
                || so.getStatus() == SubOrderStatus.COMPLETED
        );
        if (anySubOrderAlreadyProcessing) {
            throw new BusinessException("Order item can only be cancelled before vendor starts processing", HttpStatus.BAD_REQUEST, "CANNOT_CANCEL_ITEM");
        }

        boolean anyItemAlreadyHandledByVendor = order.getSubOrders().stream()
            .flatMap(so -> Optional.ofNullable(so.getItems()).orElseGet(List::of).stream())
            .anyMatch(i -> i.getStatus() != OrderItemStatus.PENDING && i.getStatus() != OrderItemStatus.CANCELLED);
        if (anyItemAlreadyHandledByVendor) {
            throw new BusinessException("Order item cannot be cancelled after vendor confirmed/returned items", HttpStatus.BAD_REQUEST, "CANNOT_CANCEL_ITEM");
        }

        SubOrder foundSubOrder = null;
        OrderItem foundItem = null;
        for (SubOrder subOrder : order.getSubOrders()) {
            List<OrderItem> items = Optional.ofNullable(subOrder.getItems()).orElseGet(List::of);
            for (OrderItem item : items) {
                if (item != null && item.getId() != null && item.getId().equals(itemId)) {
                    foundSubOrder = subOrder;
                    foundItem = item;
                    break;
                }
            }
            if (foundItem != null) {
                break;
            }
        }

        if (foundItem == null || foundSubOrder == null) {
            throw new ResourceNotFoundException("Order item not found");
        }

        if (foundSubOrder.getStatus() == SubOrderStatus.CANCELLED) {
            return orderResponseMapper.toOrderResponse(order);
        }

        if (foundItem.getStatus() == OrderItemStatus.CANCELLED) {
            return orderResponseMapper.toOrderResponse(order);
        }

        if (foundItem.getStatus() != OrderItemStatus.PENDING) {
            throw new BusinessException("Only PENDING items can be cancelled", HttpStatus.BAD_REQUEST, "CANNOT_CANCEL_ITEM");
        }

        PaymentStatus originalPaymentStatus = order.getPaymentStatus();
        BigDecimal originalTotalPayable = MoneyUtils.normalizeMoney(order.getTotalPayableAmount());

        foundItem.setStatus(OrderItemStatus.CANCELLED);

        StockRollbackRequest rollback = StockRollbackRequest.builder()
            .orderId(order.getOrderCode())
            .productId(foundItem.getProductId())
            .quantity(foundItem.getQuantity())
            .timestamp(LocalDateTime.now())
            .build();
        orderEventProducer.sendStockRollbackRequest(rollback);

        boolean allItemsCancelledInSubOrder = Optional.ofNullable(foundSubOrder.getItems()).orElseGet(List::of)
            .stream()
            .filter(Objects::nonNull)
            .allMatch(i -> i.getStatus() == OrderItemStatus.CANCELLED);
        if (allItemsCancelledInSubOrder) {
            foundSubOrder.setStatus(SubOrderStatus.CANCELLED);
            foundSubOrder.setPayoutStatus(PayoutStatus.CANCELLED);
        }

        boolean nowFullyCancelled = order.getSubOrders().stream().allMatch(so -> so.getStatus() == SubOrderStatus.CANCELLED);

        BigDecimal recalculatedDiscount = Optional.ofNullable(order.getPlatformDiscountAmount()).orElse(BigDecimal.ZERO);
        Long holdIdToReleaseBecauseIneligible = null;

        if (nowFullyCancelled) {
            recalculatedDiscount = BigDecimal.ZERO;
        } else if (order.getPlatformDiscountHoldId() != null
            && order.getPlatformDiscountCode() != null
            && !order.getPlatformDiscountCode().isBlank()) {

            double remainingSubtotal = orderAmountService.sumActiveItemSubtotal(order);
            DiscountHoldService.DiscountRecalculationResult result = discountHoldService.recalculateHold(
                customerKeycloakId,
                order,
                remainingSubtotal,
                "ORDER_ITEM_CANCELLED_RECALCULATE"
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

        syncPaymentAfterCustomerCancel(
            order,
            originalPaymentStatus,
            originalTotalPayable,
            authorizationHeader,
            nowFullyCancelled
        );

        Order saved = orderRepository.save(order);

        if (holdIdToReleaseBecauseIneligible != null) {
            DiscountCommand releaseCmd = DiscountCommand.builder()
                .type(DiscountCommandType.RELEASE_REQUESTED)
                .orderId(saved.getId())
                .userId(customerKeycloakId)
                .holdId(holdIdToReleaseBecauseIneligible)
                .createdAt(Instant.now())
                .reason("ORDER_ITEM_CANCELLED_DISCOUNT_INELIGIBLE")
                .build();

            discountCommandProducer.send(releaseCmd);
        }

        // Only release discount hold when the entire order becomes cancelled.
        if (nowFullyCancelled
            && saved.getPlatformDiscountHoldId() != null
            && saved.getPlatformDiscountCode() != null
            && !saved.getPlatformDiscountCode().isBlank()) {

            DiscountCommand releaseCmd = DiscountCommand.builder()
                .type(DiscountCommandType.RELEASE_REQUESTED)
                .orderId(saved.getId())
                .userId(customerKeycloakId)
                .code(saved.getPlatformDiscountCode())
                .holdId(saved.getPlatformDiscountHoldId())
                .createdAt(Instant.now())
                .reason("ORDER_CANCELLED")
                .build();

            discountCommandProducer.send(releaseCmd);
        }

        return orderResponseMapper.toOrderResponse(saved);
    }

    @Transactional
    public OrderResponse markMyOrderPaymentCompletedInternal(String orderCode) {
        Order order = orderRepository.findByOrderCode(orderCode)
            .orElseThrow(() -> new ResourceNotFoundException("Order not found for orderCode=" + orderCode));

        if (order.getPaymentStatus() == PaymentStatus.COMPLETED) {
            // Legacy / retry: orders created before checkout reservation still reserve here
            reserveStockForOrder(order);
            return orderResponseMapper.toOrderResponse(order);
        }

        if ( order.getPaymentStatus() != PaymentStatus.PENDING) {
            throw new BusinessException("Cannot complete payment from status=" + order.getPaymentStatus(), HttpStatus.CONFLICT,"INVALID_PAYMENT_STATE");
        }

        order.setPaymentStatus(PaymentStatus.COMPLETED);
        Order saved = orderRepository.save(order);

        reserveStockForOrder(saved);

        // Commit discount usage if a hold exists
        if (saved.getPlatformDiscountHoldId() != null
            && saved.getPlatformDiscountCode() != null
            && !saved.getPlatformDiscountCode().isBlank()) {

            DiscountCommand commitCmd = DiscountCommand.builder()
                .type(DiscountCommandType.COMMIT_REQUESTED)
                .orderId(saved.getId())
                .userId(saved.getCustomerKeycloakId())
                .code(saved.getPlatformDiscountCode())
                .holdId(saved.getPlatformDiscountHoldId())
                .createdAt(Instant.now())
                .reason("PAYMENT_COMPLETED_INTERNAL")
                .build();

            discountCommandProducer.send(commitCmd);
        }

        return orderResponseMapper.toOrderResponse(saved);
    }

    @Transactional
    public OrderResponse markMyOrderPaymentPending(String customerKeycloakId, String orderCode) {
        Order order = orderRepository.findByOrderCodeAndCustomerKeycloakId(orderCode, customerKeycloakId)
            .orElseThrow(() -> new ResourceNotFoundException("Order not found"));

        if (order.getPaymentStatus() == PaymentStatus.PENDING) {
            return orderResponseMapper.toOrderResponse(order);
        }

        order.setPaymentStatus(PaymentStatus.PENDING);
        Order saved = orderRepository.save(order);
        return orderResponseMapper.toOrderResponse(saved);
    }

    /**
     * Handle payment failed event from payment-service
     * Rolls back order and releases discount hold
     */
    @Transactional
    public OrderResponse handlePaymentFailed(String orderCode, String failureCode, String failureMessage) {
        Order order = orderRepository.findByOrderCode(orderCode)
            .orElseThrow(() -> new ResourceNotFoundException("Order not found for orderCode=" + orderCode));

        // If payment already failed or cancelled, return as-is
        if (order.getPaymentStatus() == PaymentStatus.FAILED || 
            order.getPaymentStatus() == PaymentStatus.CANCELLED) {
            return orderResponseMapper.toOrderResponse(order);
        }

        // Only fail payments that already reached CREATED/PENDING
        if (order.getPaymentStatus() != PaymentStatus.PENDING) {
            return orderResponseMapper.toOrderResponse(order);
        }

        order.setPaymentStatus(PaymentStatus.FAILED);
        Order saved = orderRepository.save(order);

        rollbackStockForPaymentFailure(saved);

        // Release discount hold if present
        if (saved.getPlatformDiscountHoldId() != null
            && saved.getPlatformDiscountCode() != null
            && !saved.getPlatformDiscountCode().isBlank()) {

            DiscountCommand releaseCmd = DiscountCommand.builder()
                .type(DiscountCommandType.RELEASE_REQUESTED)
                .orderId(saved.getId())
                .userId(saved.getCustomerKeycloakId())
                .code(saved.getPlatformDiscountCode())
                .holdId(saved.getPlatformDiscountHoldId())
                .createdAt(Instant.now())
                .reason("PAYMENT_FAILED: " + failureCode)
                .build();

            discountCommandProducer.send(releaseCmd);
        }

        return orderResponseMapper.toOrderResponse(saved);
    }

    private void rollbackStockForPaymentFailure(Order order) {
        if (order == null || !order.isStockReservationSent()) {
            return;
        }

        List<SubOrder> subOrders = Optional.ofNullable(order.getSubOrders()).orElseGet(List::of);
        if (subOrders.isEmpty()) {
            log.warn("Skip stock rollback for payment failure because order has no suborders: orderCode={}", order.getOrderCode());
            return;
        }

        for (SubOrder subOrder : subOrders) {
            List<OrderItem> items = Optional.ofNullable(subOrder.getItems()).orElseGet(List::of);
            for (OrderItem item : items) {
                if (item == null || item.getProductId() == null || item.getQuantity() == null || item.getQuantity() <= 0) {
                    continue;
                }

                StockRollbackRequest rollback = StockRollbackRequest.builder()
                    .orderId(order.getOrderCode())
                    .productId(item.getProductId())
                    .quantity(item.getQuantity())
                    .timestamp(LocalDateTime.now())
                    .build();
                orderEventProducer.sendStockRollbackRequest(rollback);
                log.info("Sent stock rollback for failed payment: orderCode={} productId={} quantity={}",
                    order.getOrderCode(), item.getProductId(), item.getQuantity());
            }
        }
    }

    private void reserveStockAtCheckoutOrThrow(Order order) {
        if (order == null || order.getOrderCode() == null || order.getOrderCode().isBlank()) {
            return;
        }

        int expected = countReservableItems(order);
        if (expected <= 0) {
            return;
        }

        stockReservationAwaiter.register(order.getOrderCode());
        reserveStockForOrder(order);

        if (!order.isStockReservationSent()) {
            stockReservationAwaiter.cancel(order.getOrderCode());
            throw new BusinessException(
                "Failed to reserve stock for this order",
                HttpStatus.CONFLICT,
                "STOCK_RESERVATION_NOT_SENT"
            );
        }

        try {
            boolean reserved = stockReservationAwaiter.await(order.getOrderCode());
            if (!reserved) {
                throw new BusinessException(
                    "Insufficient stock for one or more products",
                    HttpStatus.CONFLICT,
                    "INSUFFICIENT_STOCK"
                );
            }
        } catch (TimeoutException ex) {
            stockReservationAwaiter.cancel(order.getOrderCode());
            orderCompensationService.compensateStockReservationTimeout(order);
            throw new BusinessException(
                "Stock reservation timed out",
                HttpStatus.SERVICE_UNAVAILABLE,
                "STOCK_RESERVATION_TIMEOUT"
            );
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            stockReservationAwaiter.cancel(order.getOrderCode());
            orderCompensationService.compensateStockReservationTimeout(order);
            throw new BusinessException(
                "Stock reservation was interrupted",
                HttpStatus.SERVICE_UNAVAILABLE,
                "STOCK_RESERVATION_INTERRUPTED"
            );
        }
    }

    private int countReservableItems(Order order) {
        int count = 0;
        for (SubOrder subOrder : Optional.ofNullable(order.getSubOrders()).orElseGet(List::of)) {
            for (OrderItem item : Optional.ofNullable(subOrder.getItems()).orElseGet(List::of)) {
                if (item != null && item.getProductId() != null && item.getQuantity() != null && item.getQuantity() > 0) {
                    count++;
                }
            }
        }
        return count;
    }

    public void reserveStockForOrder(Order order) {
        if (order == null || order.getOrderCode() == null || order.getOrderCode().isBlank()) {
            return;
        }

        // Checkout reserves while PENDING; payment-success is a no-op once stockReservationSent
        if (order.getPaymentStatus() != PaymentStatus.PENDING && order.getPaymentStatus() != PaymentStatus.COMPLETED) {
            log.warn("Skip stock reservation because payment status is not reservable: orderCode={} paymentStatus={}",
                order.getOrderCode(), order.getPaymentStatus());
            return;
        }

        // Check if already sent - exactly-once delivery guarantee
        if (order.isStockReservationSent()) {
            log.info("Stock reservation already sent for orderCode={}, skipping", order.getOrderCode());
            return;
        }

        List<StockReservationRequest> reservationRequests = new ArrayList<>();
        for (SubOrder subOrder : Optional.ofNullable(order.getSubOrders()).orElseGet(List::of)) {
            for (OrderItem item : Optional.ofNullable(subOrder.getItems()).orElseGet(List::of)) {
                if (item == null || item.getProductId() == null || item.getQuantity() == null) {
                    continue;
                }

                StockReservationRequest stockReq = StockReservationRequest.builder()
                    .orderId(order.getOrderCode())
                    .productId(item.getProductId())
                    .quantity(item.getQuantity())
                    .timestamp(LocalDateTime.now())
                    .build();
                reservationRequests.add(stockReq);
            }
        }

        if (reservationRequests.isEmpty()) {
            return;
        }

        // Send all stock reservation requests to Kafka
        for (StockReservationRequest reservationRequest : reservationRequests) {
            orderEventProducer.sendStockReservationRequest(reservationRequest);
            log.debug("Sent stock reservation request: orderCode={} productId={} quantity={}",
                reservationRequest.getOrderId(), reservationRequest.getProductId(), reservationRequest.getQuantity());
        }

        // Mark in DB that stock reservation has been sent (exactly-once delivery)
        // This ensures if consumer crashes after Kafka send but before markAsProcessed,
        // on retry the duplicate check (isStockReservationSent) will prevent duplicate sends
        order.setStockReservationSent(true);
        Order saved = orderRepository.save(order);
        log.info("Stock reservation marked as sent for orderCode={}", saved.getOrderCode());
    }

    public void reserveStockForOrder(String orderCode) {
        if (orderCode == null || orderCode.isBlank()) {
            return;
        }

        Order order = orderRepository.findByOrderCode(orderCode)
            .orElseThrow(() -> new ResourceNotFoundException("Order not found for orderCode=" + orderCode));

        reserveStockForOrder(order);
    }
}

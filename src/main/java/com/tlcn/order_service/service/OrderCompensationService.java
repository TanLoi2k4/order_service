package com.tlcn.order_service.service;

import com.tlcn.order_service.dto.RefundRequest;
import com.tlcn.order_service.dto.kafka.DiscountCommand;
import com.tlcn.order_service.dto.kafka.DiscountCommandType;
import com.tlcn.order_service.enums.OrderItemStatus;
import com.tlcn.order_service.enums.PaymentMethod;
import com.tlcn.order_service.enums.PaymentStatus;
import com.tlcn.order_service.enums.PayoutStatus;
import com.tlcn.order_service.enums.SubOrderStatus;
import com.tlcn.order_service.dto.kafka.StockRollbackRequest;
import com.tlcn.order_service.kafka.DiscountCommandProducer;
import com.tlcn.order_service.kafka.OrderEventProducer;
import com.tlcn.order_service.model.Order;
import com.tlcn.order_service.model.OrderItem;
import com.tlcn.order_service.model.SubOrder;
import com.tlcn.order_service.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Compensating actions for checkout / stock saga failures.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OrderCompensationService {

    public static final String STOCK_OK_KEY_PREFIX = "kafka:order:stock:ok:";
    public static final String STOCK_SEEN_KEY_PREFIX = "kafka:order:stock:seen:";

    private final OrderRepository orderRepository;
    private final DiscountCommandProducer discountCommandProducer;
    private final OrderEventProducer orderEventProducer;
    private final RedisTemplate<String, String> redisTemplate;

    /**
     * After order was persisted but payment-service create failed:
     * cancel sub-orders, mark payment FAILED, release discount hold.
     * Uses REQUIRES_NEW so the failure state is committed even when the caller rethrows.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Order compensatePaymentCreateFailure(Order order, String reason) {
        Order managed = orderRepository.findById(order.getId())
            .orElse(order);

        if (managed.getPaymentStatus() == PaymentStatus.FAILED
            || managed.getPaymentStatus() == PaymentStatus.CANCELLED
            || managed.getPaymentStatus() == PaymentStatus.REFUNDED) {
            log.info("Skip payment-create compensation; already terminal: orderCode={} status={}",
                managed.getOrderCode(), managed.getPaymentStatus());
            return managed;
        }

        cancelSubOrdersAndItems(managed);
        rollbackReservedStock(managed);
        managed.setPaymentStatus(PaymentStatus.FAILED);
        releaseDiscountHold(managed, reason);
        Order saved = orderRepository.save(managed);
        log.warn("Compensated payment-create failure: orderCode={} reason={}", saved.getOrderCode(), reason);
        return saved;
    }

    /**
     * Checkout timed out waiting for stock replies. Cancel the order and release
     * reservations already recorded in Redis; in-flight successes are rolled back
     * when their replies arrive against a terminal order.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Order compensateStockReservationTimeout(Order order) {
        rollbackRecordedSuccessfulReservations(order == null ? null : order.getOrderCode());
        return compensateStockReservationFailure(order);
    }

    /**
     * After stock reservation failed (payment already completed):
     * ensure sub-orders cancelled, release discount, refund prepaid payment.
     */
    @Transactional
    public Order compensateStockReservationFailure(Order order) {
        Order managed = orderRepository.findById(order.getId())
            .orElse(order);

        if (managed.getPaymentStatus() == PaymentStatus.REFUNDED
            || managed.getPaymentStatus() == PaymentStatus.FAILED
            || managed.getPaymentStatus() == PaymentStatus.CANCELLED) {
            log.info("Skip stock-reservation compensation; already terminal: orderCode={} status={}",
                managed.getOrderCode(), managed.getPaymentStatus());
            releaseDiscountHold(managed, "STOCK_RESERVATION_FAILED");
            return managed;
        }

        cancelSubOrdersAndItems(managed);
        releaseDiscountHold(managed, "STOCK_RESERVATION_FAILED");

        PaymentStatus status = managed.getPaymentStatus();
        if (status == PaymentStatus.COMPLETED || status == PaymentStatus.PARTIALLY_REFUNDED) {
            if (managed.getPaymentMethod() != PaymentMethod.COD) {
                sendFullRefund(managed, "STOCK_RESERVATION_FAILED");
                managed.setPaymentStatus(PaymentStatus.REFUNDED);
            } else {
                // COD should not normally be COMPLETED before stock reserve; mark failed locally.
                managed.setPaymentStatus(PaymentStatus.FAILED);
            }
        } else if (status == PaymentStatus.PENDING) {
            managed.setPaymentStatus(PaymentStatus.FAILED);
        }

        Order saved = orderRepository.save(managed);
        log.warn("Compensated stock-reservation failure: orderCode={} paymentStatus={}",
            saved.getOrderCode(), saved.getPaymentStatus());
        return saved;
    }

    /**
     * Release a hold that succeeded before the order row was persisted (DB failure after HOLD).
     */
    public void releaseOrphanDiscountHold(String userId, String code, Long holdId, String reason) {
        if (holdId == null) {
            return;
        }

        DiscountCommand releaseCmd = DiscountCommand.builder()
            .type(DiscountCommandType.RELEASE_REQUESTED)
            .orderId(null)
            .userId(userId)
            .code(code)
            .holdId(holdId)
            .createdAt(Instant.now())
            .reason(reason)
            .build();

        discountCommandProducer.send(releaseCmd);
        log.info("Sent orphan discount RELEASE: holdId={} userId={} reason={}", holdId, userId, reason);
    }

    public void releaseDiscountHold(Order order, String reason) {
        if (order == null
            || order.getPlatformDiscountHoldId() == null
            || order.getPlatformDiscountCode() == null
            || order.getPlatformDiscountCode().isBlank()) {
            return;
        }

        DiscountCommand releaseCmd = DiscountCommand.builder()
            .type(DiscountCommandType.RELEASE_REQUESTED)
            .orderId(order.getId())
            .userId(order.getCustomerKeycloakId())
            .code(order.getPlatformDiscountCode())
            .holdId(order.getPlatformDiscountHoldId())
            .createdAt(Instant.now())
            .reason(reason)
            .build();

        discountCommandProducer.send(releaseCmd);
        log.info("Sent discount RELEASE: orderCode={} holdId={} reason={}",
            order.getOrderCode(), order.getPlatformDiscountHoldId(), reason);
    }

    public void rollbackReservedStock(Order order) {
        if (order == null || !order.isStockReservationSent()) {
            return;
        }

        for (SubOrder subOrder : Optional.ofNullable(order.getSubOrders()).orElseGet(List::of)) {
            for (OrderItem item : Optional.ofNullable(subOrder.getItems()).orElseGet(List::of)) {
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
            }
        }
        log.info("Sent stock rollback for reserved order: orderCode={}", order.getOrderCode());
    }

    public void rollbackRecordedSuccessfulReservations(String orderCode) {
        if (orderCode == null || orderCode.isBlank()) {
            return;
        }
        String okKey = STOCK_OK_KEY_PREFIX + orderCode;
        Set<String> okItems = redisTemplate.opsForSet().members(okKey);
        if (okItems == null || okItems.isEmpty()) {
            redisTemplate.delete(STOCK_SEEN_KEY_PREFIX + orderCode);
            redisTemplate.delete(okKey);
            return;
        }

        for (String itemKey : okItems) {
            ParsedItem parsed = parseItemKey(itemKey);
            if (parsed == null) {
                continue;
            }
            StockRollbackRequest rollback = StockRollbackRequest.builder()
                .orderId(orderCode)
                .productId(parsed.productId)
                .quantity(parsed.quantity)
                .timestamp(LocalDateTime.now())
                .build();
            orderEventProducer.sendStockRollbackRequest(rollback);
        }
        redisTemplate.delete(STOCK_SEEN_KEY_PREFIX + orderCode);
        redisTemplate.delete(okKey);
        log.info("Rolled back recorded stock reservations: orderCode={} count={}", orderCode, okItems.size());
    }

    public static boolean isStockSagaAbandoned(Order order) {
        if (order == null) {
            return false;
        }
        PaymentStatus paymentStatus = order.getPaymentStatus();
        if (paymentStatus == PaymentStatus.FAILED
            || paymentStatus == PaymentStatus.CANCELLED
            || paymentStatus == PaymentStatus.REFUNDED) {
            return true;
        }
        List<SubOrder> subOrders = Optional.ofNullable(order.getSubOrders()).orElseGet(List::of);
        return !subOrders.isEmpty()
            && subOrders.stream().allMatch(so -> so != null && so.getStatus() == SubOrderStatus.CANCELLED);
    }

    private static ParsedItem parseItemKey(String itemKey) {
        if (itemKey == null || itemKey.isBlank()) {
            return null;
        }
        String[] parts = itemKey.split(":", 2);
        if (parts.length != 2) {
            return null;
        }
        try {
            Long productId = Long.valueOf(parts[0]);
            Integer qty = Integer.valueOf(parts[1]);
            if (qty <= 0) {
                return null;
            }
            return new ParsedItem(productId, qty);
        } catch (Exception e) {
            return null;
        }
    }

    private record ParsedItem(Long productId, Integer quantity) {}

    private void cancelSubOrdersAndItems(Order order) {
        if (order.getSubOrders() == null) {
            return;
        }
        for (SubOrder subOrder : order.getSubOrders()) {
            if (subOrder.getStatus() != SubOrderStatus.CANCELLED) {
                subOrder.setStatus(SubOrderStatus.CANCELLED);
            }
            subOrder.setPayoutStatus(PayoutStatus.CANCELLED);

            List<OrderItem> items = Optional.ofNullable(subOrder.getItems()).orElseGet(List::of);
            for (OrderItem item : items) {
                if (item == null) {
                    continue;
                }
                if (item.getStatus() != OrderItemStatus.CANCELLED && item.getStatus() != OrderItemStatus.RETURNED) {
                    item.setStatus(OrderItemStatus.CANCELLED);
                }
            }
        }
    }

    private void sendFullRefund(Order order, String reason) {
        BigDecimal amount = order.getTotalPayableAmount();
        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            log.info("Skip refund for orderCode={}: amount is zero", order.getOrderCode());
            return;
        }

        RefundRequest refundRequest = RefundRequest.builder()
            .orderCode(order.getOrderCode())
            .orderId(order.getId())
            .customerKeycloakId(order.getCustomerKeycloakId())
            .amount(amount.setScale(2, RoundingMode.HALF_UP))
            .currency(order.getCurrency() == null || order.getCurrency().isBlank() ? "VND" : order.getCurrency())
            .reason(reason)
            .requestedAt(Instant.now())
            .build();

        orderEventProducer.sendPaymentRefundRequest(refundRequest);
        log.info("Sent payment-refund-request for stock failure: orderCode={} amount={}",
            order.getOrderCode(), refundRequest.getAmount());
    }
}

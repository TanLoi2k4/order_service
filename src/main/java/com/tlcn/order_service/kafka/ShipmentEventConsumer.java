package com.tlcn.order_service.kafka;

import com.tlcn.order_service.client.PaymentClient;
import com.tlcn.order_service.dto.RefundRequest;
import com.tlcn.order_service.dto.kafka.OrderCompleted;
import com.tlcn.order_service.dto.kafka.SubOrderPayoutRequested;
import com.tlcn.order_service.dto.kafka.ShipmentCreated;
import com.tlcn.order_service.dto.kafka.ShipmentStatusChanged;
import com.tlcn.order_service.enums.OrderItemStatus;
import com.tlcn.order_service.enums.PaymentMethod;
import com.tlcn.order_service.enums.PaymentStatus;
import com.tlcn.order_service.enums.PayoutStatus;
import com.tlcn.order_service.enums.ShipmentStatus;
import com.tlcn.order_service.enums.SubOrderStatus;
import com.tlcn.order_service.model.Order;
import com.tlcn.order_service.model.Shipment;
import com.tlcn.order_service.model.SubOrder;
import com.tlcn.order_service.service.OrderCompletedEnrichmentService;
import com.tlcn.order_service.service.ReturnRequestService;
import com.tlcn.order_service.repository.OrderRepository;
import com.tlcn.order_service.repository.ShipmentRepository;
import com.tlcn.order_service.repository.SubOrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class ShipmentEventConsumer {

    private static final String IDEMPOTENCY_KEY_PREFIX = "kafka:shipment:idempotency:";
    private static final String ORDER_COMPLETED_IDEMPOTENCY_PREFIX = "kafka:order-completed:idempotency:";

    private final SubOrderRepository subOrderRepository;
    private final OrderRepository orderRepository;
    private final ShipmentRepository shipmentRepository;
    private final OrderEventProducer orderEventProducer;
    private final OrderCompletedEnrichmentService orderCompletedEnrichmentService;
    private final ReturnRequestService returnRequestService;
    private final PaymentClient paymentClient;
    private final RedisTemplate<String, String> redisTemplate;

    @Value("${kafka.idempotency.ttl-hours:24}")
    private long idempotencyTtlHours;

    @KafkaListener(
        topics = "${spring.kafka.topics.shipment-created:shipment-created}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeShipmentCreated(
        ShipmentCreated event,
        @Header(name = "messageId", required = false) String messageId,
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset,
        Acknowledgment ack
    ) {
        try {
            if (event == null || event.getSubOrderId() == null) {
                log.warn("Ignore invalid shipment-created event: messageId={} topic={} partition={} offset={}", messageId, topic, partition, offset);
                ack.acknowledge();
                return;
            }

            String eventId = normalizeEventId(event.getEventId(), messageId);
            if (isReturnShipment(event.getShipmentType(), event.getReturnRequestId())) {
                returnRequestService.applyCarrierEvent(
                    event.getReturnRequestId(),
                    event.getSubOrderId(),
                    event.getShipmentId(),
                    event.getExternalShipmentId(),
                    event.getCarrierCode(),
                    event.getStatus()
                );
                completeSuccessfully(eventId, ack);
                return;
            }
            // Upsert is idempotent. Re-apply even on Redis hits so a mark from a rolled-back
            // attempt cannot leave the local shipment projection stale.
            applyShipmentCreated(event, eventId, isAlreadyProcessed(eventId));
            completeSuccessfully(eventId, ack);
        } catch (Exception e) {
            log.error("Failed to process shipment-created: subOrderId={} messageId={} error={}",
                event == null ? null : event.getSubOrderId(),
                messageId,
                e.getMessage(),
                e
            );
            throw e;
        }
    }

    @KafkaListener(
        topics = "${spring.kafka.topics.shipment-status-changed:shipment-status-changed}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeShipmentStatusChanged(
        ShipmentStatusChanged event,
        @Header(name = "messageId", required = false) String messageId,
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset,
        Acknowledgment ack
    ) {
        try {
            if (event == null || event.getSubOrderId() == null || event.getStatus() == null) {
                log.warn("Ignore invalid shipment-status-changed event: messageId={} topic={} partition={} offset={}", messageId, topic, partition, offset);
                ack.acknowledge();
                return;
            }

            String eventId = normalizeEventId(event.getEventId(), messageId);
            if (isAlreadyProcessed(eventId)) {
                log.info("Skip duplicate shipment-status-changed: eventId={} subOrderId={}", eventId, event.getSubOrderId());
                ack.acknowledge();
                return;
            }

            if (isReturnShipment(event.getShipmentType(), event.getReturnRequestId())) {
                returnRequestService.applyCarrierEvent(
                    event.getReturnRequestId(),
                    event.getSubOrderId(),
                    event.getShipmentId(),
                    event.getExternalShipmentId(),
                    null,
                    event.getStatus()
                );
                completeSuccessfully(eventId, ack);
                return;
            }

            SubOrder subOrder = subOrderRepository.findById(event.getSubOrderId()).orElse(null);
            if (subOrder == null) {
                log.warn("SubOrder not found for shipment-status-changed: subOrderId={} eventId={}", event.getSubOrderId(), eventId);
                completeSuccessfully(eventId, ack);
                return;
            }

            upsertShipmentProjection(
                subOrder,
                event.getExternalShipmentId(),
                null,
                event.getStatus(),
                event.getMocked()
            );

            SubOrderStatus targetStatus = mapShipmentStatus(event.getStatus());
            if (targetStatus == null) {
                log.warn("Unsupported shipment status for subOrder update: status={} shipmentId={} subOrderId={}", event.getStatus(), event.getShipmentId(), event.getSubOrderId());
                completeSuccessfully(eventId, ack);
                return;
            }

            SubOrderStatus currentStatus = subOrder.getStatus();
            if (currentStatus != null && currentStatus.isTerminal()) {
                log.info("Ignore shipment-status-changed because subOrder is terminal: subOrderId={} currentStatus={} eventStatus={} eventId={}", event.getSubOrderId(), currentStatus, event.getStatus(), eventId);
                completeSuccessfully(eventId, ack);
                return;
            }

            if (currentStatus != null && targetStatus.progressRank() <= currentStatus.progressRank()) {
                log.info("Ignore stale subOrder update from shipment-status-changed: subOrderId={} currentStatus={} eventStatus={} eventId={}", event.getSubOrderId(), currentStatus, event.getStatus(), eventId);
                completeSuccessfully(eventId, ack);
                return;
            }

            Order order = subOrder.getOrder();
            subOrder.setStatus(targetStatus);
            if (targetStatus == SubOrderStatus.COMPLETED && subOrder.getCompletedAt() == null) {
                subOrder.setCompletedAt(Instant.now());
            }

            if (targetStatus == SubOrderStatus.COMPLETED && subOrder.getPayoutStatus() == PayoutStatus.PENDING) {
                subOrder.setPayoutStatus(PayoutStatus.PAID);
            }

            if ((targetStatus == SubOrderStatus.CANCELLED || targetStatus == SubOrderStatus.RETURNED)
                && subOrder.getPayoutStatus() == PayoutStatus.PENDING) {
                subOrder.setPayoutStatus(PayoutStatus.CANCELLED);
            }

            subOrderRepository.save(subOrder);

            if (order != null && targetStatus == SubOrderStatus.COMPLETED) {
                BigDecimal grossAmountBeforeDiscount = subOrder.getItems().stream()
                    .filter(item -> item != null && item.getStatus() != OrderItemStatus.CANCELLED && item.getStatus() != OrderItemStatus.RETURNED)
                    .map(item -> item.getLineTotal() == null ? BigDecimal.ZERO : item.getLineTotal())
                    .reduce(BigDecimal.ZERO, BigDecimal::add);

                BigDecimal commissionFee = grossAmountBeforeDiscount
                    .multiply(BigDecimal.valueOf(0.1))
                    .setScale(2, RoundingMode.HALF_UP);

                subOrder.setCommissionFee(commissionFee);
                subOrderRepository.save(subOrder);

                BigDecimal platformPayableToVendor = BigDecimal.ZERO;
                BigDecimal vendorPayableToPlatform = BigDecimal.ZERO;

                if (order.getPaymentMethod() == PaymentMethod.VNPAY || order.getPaymentMethod() == PaymentMethod.WALLET) {
                    platformPayableToVendor = grossAmountBeforeDiscount;
                    vendorPayableToPlatform = commissionFee;
                } else if (order.getPaymentMethod() == PaymentMethod.COD) {
                    vendorPayableToPlatform = commissionFee;
                }

                SubOrderPayoutRequested payoutRequested = SubOrderPayoutRequested.builder()
                    .orderCode(order.getOrderCode())
                    .orderId(order.getId())
                    .subOrderId(subOrder.getId())
                    .vendorKeycloakId(subOrder.getVendorKeycloakId())
                    .paymentMethod(order.getPaymentMethod())
                    .currency(order.getCurrency())
                    .grossAmountBeforeDiscount(grossAmountBeforeDiscount)
                    .commissionFee(commissionFee)
                    .platformPayableToVendor(platformPayableToVendor)
                    .vendorPayableToPlatform(vendorPayableToPlatform)
                    .createdAt(Instant.now())
                    .build();

                orderEventProducer.sendSubOrderPayoutRequested(payoutRequested);
            }

            if (order != null && isCodOrder(order) && targetStatus == SubOrderStatus.COMPLETED) {
                if (allChargeableSubOrdersCompleted(order.getId()) && order.getPaymentStatus() != PaymentStatus.COMPLETED) {
                    // Source of truth: payment-service completes COD, then publishes payment-success
                    paymentClient.completeCodPayment(order.getOrderCode());
                    order.setPaymentStatus(PaymentStatus.COMPLETED);
                    orderRepository.save(order);
                    log.info("COD payment completed via payment-service: orderCode={}", order.getOrderCode());
                }
            }

            if (order != null && targetStatus == SubOrderStatus.COMPLETED) {
                tryPublishOrderCompleted(order);
            }

            if (order != null && !isCodOrder(order)
                && (targetStatus == SubOrderStatus.CANCELLED || targetStatus == SubOrderStatus.RETURNED)
                && order.getPaymentStatus() != PaymentStatus.REFUNDED) {
                RefundRequest refundRequest = buildRefundRequest(order, subOrder, event.getShipmentId());
                orderEventProducer.sendPaymentRefundRequest(refundRequest);
                refreshPrepaidRefundStatus(order);
                orderRepository.save(order);
            }

            log.info("Applied shipment-status-changed: subOrderId={} shipmentId={} status={} -> subOrderStatus={}",
                event.getSubOrderId(), event.getShipmentId(), event.getStatus(), subOrder.getStatus());
            completeSuccessfully(eventId, ack);
        } catch (Exception e) {
            log.error("Failed to process shipment-status-changed: subOrderId={} messageId={} error={}",
                event == null ? null : event.getSubOrderId(),
                messageId,
                e.getMessage(),
                e
            );
            throw e;
        }
    }

    private void applyShipmentCreated(ShipmentCreated event, String eventId, boolean duplicate) {
        SubOrder subOrder = subOrderRepository.findById(event.getSubOrderId()).orElse(null);
        if (subOrder == null) {
            log.warn("SubOrder not found for shipment-created: subOrderId={} eventId={}", event.getSubOrderId(), eventId);
            return;
        }

        SubOrderStatus currentStatus = subOrder.getStatus();
        if (currentStatus != null && currentStatus.isTerminal()) {
            log.info("Ignore shipment-created because subOrder is terminal: subOrderId={} currentStatus={} eventId={}", event.getSubOrderId(), currentStatus, eventId);
        } else if (currentStatus == null || currentStatus.progressRank() < SubOrderStatus.SHIPPING.progressRank()) {
            subOrder.setStatus(SubOrderStatus.SHIPPING);
            subOrderRepository.save(subOrder);
            log.info("Applied shipment-created: subOrderId={} shipmentId={} externalShipmentId={} -> SHIPPING", event.getSubOrderId(), event.getShipmentId(), event.getExternalShipmentId());
        } else if (duplicate) {
            log.info("Re-apply shipment-created projection after duplicate delivery: eventId={} subOrderId={}", eventId, event.getSubOrderId());
        } else {
            log.info("Ignore shipment-created because subOrder is already at or beyond SHIPPING: subOrderId={} currentStatus={} eventId={}", event.getSubOrderId(), currentStatus, eventId);
        }

        upsertShipmentProjection(
            subOrder,
            event.getExternalShipmentId(),
            event.getCarrierCode(),
            event.getStatus(),
            event.getMocked()
        );
    }

    private boolean isReturnShipment(String shipmentType, Long returnRequestId) {
        return returnRequestId != null || (shipmentType != null && "RETURN".equalsIgnoreCase(shipmentType.trim()));
    }

    private String normalizeEventId(String eventId, String fallback) {
        if (eventId != null && !eventId.isBlank()) {
            return eventId;
        }
        return fallback == null || fallback.isBlank() ? null : fallback;
    }

    private boolean isAlreadyProcessed(String key) {
        return key != null && Boolean.TRUE.equals(redisTemplate.hasKey(buildKey(key)));
    }

    /**
     * Redis + Kafka ack must happen after the DB transaction commits. Otherwise a constraint
     * failure marks the event processed and the retry is skipped with a stale projection.
     */
    private void completeSuccessfully(String eventId, Acknowledgment ack) {
        Runnable done = () -> {
            markAsProcessedNow(eventId);
            if (ack != null) {
                ack.acknowledge();
            }
        };
        if (TransactionSynchronizationManager.isSynchronizationActive()
            && TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    done.run();
                }
            });
            return;
        }
        done.run();
    }

    private void markAsProcessedNow(String key) {
        if (key == null || key.isBlank()) {
            return;
        }
        redisTemplate.opsForValue().set(buildKey(key), "processed", Math.max(1L, idempotencyTtlHours), TimeUnit.HOURS);
    }

    private String buildKey(String key) {
        return IDEMPOTENCY_KEY_PREFIX + key;
    }

    private void upsertShipmentProjection(
        SubOrder subOrder,
        String externalShipmentId,
        String carrierCode,
        String status,
        Boolean mocked
    ) {
        Instant now = Instant.now();
        Shipment shipment = shipmentRepository.findBySubOrder_Id(subOrder.getId()).orElse(null);
        if (shipment == null) {
            shipment = new Shipment();
            shipment.setSubOrder(subOrder);
            shipment.setCreatedAt(now);
        } else {
            shipment.setSubOrder(subOrder);
        }
        if (externalShipmentId != null && !externalShipmentId.isBlank()) {
            shipment.setExternalShipmentId(externalShipmentId);
        }
        if (carrierCode != null && !carrierCode.isBlank()) {
            shipment.setCarrierCode(carrierCode);
        }
        ShipmentStatus nextStatus = mapShipmentEventStatus(status);
        if (shipment.getStatus() == null || nextStatus.progressRank() >= shipment.getStatus().progressRank()) {
            shipment.setStatus(nextStatus);
        }
        shipment.setMocked(Boolean.TRUE.equals(mocked));
        shipment.setUpdatedAt(now);
        shipmentRepository.save(shipment);
    }

    private SubOrderStatus mapShipmentStatus(String status) {
        if (status == null || status.isBlank()) {
            return null;
        }

        return switch (status.trim().toUpperCase()) {
            case "DELIVERED" -> SubOrderStatus.COMPLETED;
            case "RETURNING" -> SubOrderStatus.RETURNING;
            case "RETURNED" -> SubOrderStatus.RETURNED;
            case "BOOKING_FAILED", "CANCELLED" -> SubOrderStatus.CANCELLED;
            case "CREATED", "BOOKED", "PICKED_UP", "IN_TRANSIT", "FAILED_DELIVERY", "PENDING", "DELIVERING" -> SubOrderStatus.SHIPPING;
            default -> null;
        };
    }

    private ShipmentStatus mapShipmentEventStatus(String status) {
        if (status == null || status.isBlank()) {
            return ShipmentStatus.CREATED;
        }

        return switch (status.trim().toUpperCase()) {
            case "CREATED" -> ShipmentStatus.CREATED;
            case "BOOKING_FAILED" -> ShipmentStatus.BOOKING_FAILED;
            case "BOOKED", "PENDING" -> ShipmentStatus.BOOKED;
            case "PICKED_UP" -> ShipmentStatus.PICKED_UP;
            case "IN_TRANSIT", "DELIVERING" -> ShipmentStatus.IN_TRANSIT;
            case "DELIVERED" -> ShipmentStatus.DELIVERED;
            case "FAILED_DELIVERY" -> ShipmentStatus.FAILED_DELIVERY;
            case "RETURNING" -> ShipmentStatus.RETURNING;
            case "RETURNED" -> ShipmentStatus.RETURNED;
            case "CANCELLED" -> ShipmentStatus.CANCELLED;
            default -> ShipmentStatus.CREATED;
        };
    }

    private boolean isCodOrder(Order order) {
        return order != null && order.getPaymentMethod() == PaymentMethod.COD;
    }

    private boolean isChargeableSubOrder(SubOrder subOrder) {
        if (subOrder == null || subOrder.getStatus() == null) {
            return false;
        }
        return subOrder.getStatus() != SubOrderStatus.CANCELLED && subOrder.getStatus() != SubOrderStatus.RETURNED;
    }

    private boolean allChargeableSubOrdersCompleted(Long orderId) {
        List<SubOrder> subOrders = subOrderRepository.findByOrder_Id(orderId);
        List<SubOrder> chargeable = subOrders.stream().filter(this::isChargeableSubOrder).toList();
        if (chargeable.isEmpty()) {
            return false;
        }
        return chargeable.stream().allMatch(so -> so.getStatus() == SubOrderStatus.COMPLETED);
    }

    private void tryPublishOrderCompleted(Order order) {
        if (order == null || order.getOrderCode() == null) {
            return;
        }

        String publishKey = ORDER_COMPLETED_IDEMPOTENCY_PREFIX + order.getOrderCode();
        if (Boolean.TRUE.equals(redisTemplate.hasKey(publishKey))) {
            return;
        }

        if (!allChargeableSubOrdersCompleted(order.getId())) {
            return;
        }

        OrderCompleted completedEvent = orderCompletedEnrichmentService.buildOrderCompletedEvent(order);
        if (completedEvent == null || completedEvent.getSubOrders() == null || completedEvent.getSubOrders().isEmpty()) {
            return;
        }

        orderEventProducer.sendOrderCompleted(completedEvent);
        redisTemplate.opsForValue().set(publishKey, "published", Math.max(1L, idempotencyTtlHours), TimeUnit.HOURS);
        log.info("Published order-completed for orderCode={}", order.getOrderCode());
    }

    private RefundRequest buildRefundRequest(Order order, SubOrder subOrder, Long shipmentId) {
        if (order == null || subOrder == null) {
            return null;
        }

        String shipmentIdText = shipmentId == null ? "UNKNOWN" : shipmentId.toString();
        return RefundRequest.builder()
            .orderCode(order.getOrderCode())
            .orderId(order.getId())
            .customerKeycloakId(order.getCustomerKeycloakId())
            .amount(subOrder.getFinalAmount())
            .currency(order.getCurrency())
            .reason("SHIPMENT_FAILED: " + shipmentIdText)
            .requestedAt(java.time.Instant.now())
            .build();
    }

    private void refreshPrepaidRefundStatus(Order order) {
        if (order == null || order.getSubOrders() == null) {
            return;
        }

        boolean allTerminal = order.getSubOrders().stream()
            .allMatch(subOrder -> subOrder.getStatus() != null && subOrder.getStatus().isTerminal());

        boolean allFailed = order.getSubOrders().stream()
            .allMatch(subOrder -> subOrder.getStatus() == SubOrderStatus.CANCELLED || subOrder.getStatus() == SubOrderStatus.RETURNED);

        if (allTerminal) {
            order.setPaymentStatus(allFailed ? PaymentStatus.REFUNDED : PaymentStatus.PARTIALLY_REFUNDED);
            return;
        }

        if (order.getPaymentStatus() == PaymentStatus.COMPLETED) {
            order.setPaymentStatus(PaymentStatus.PARTIALLY_REFUNDED);
        }
    }
}

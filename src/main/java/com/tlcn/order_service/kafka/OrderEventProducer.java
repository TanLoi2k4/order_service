package com.tlcn.order_service.kafka;

import com.tlcn.order_service.dto.RefundRequest;
import com.tlcn.order_service.dto.kafka.StockReservationRequest;
import com.tlcn.order_service.dto.kafka.StockRollbackRequest;
import com.tlcn.order_service.dto.kafka.OrderCompleted;
import com.tlcn.order_service.dto.kafka.ReturnRequestChanged;
import com.tlcn.order_service.dto.kafka.SubOrderPayoutRequested;
import com.tlcn.order_service.dto.kafka.SubOrderShippingRequested;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${spring.kafka.topics.suborder-shipping-requested:suborder-shipping-requested}")
    private String suborderShippingRequestedTopic;

    @Value("${spring.kafka.topics.stock-reservation-request:stock-reservation-request}")
    private String stockReservationRequestTopic;

    @Value("${spring.kafka.topics.stock-rollback-request:stock-rollback-request}")
    private String stockRollbackRequestTopic;

    @Value("${spring.kafka.topics.payment-refund-request:payment-refund-request}")
    private String paymentRefundRequestTopic;

    @Value("${spring.kafka.topics.suborder-payout-requested:suborder-payout-requested}")
    private String suborderPayoutRequestedTopic;

    @Value("${spring.kafka.topics.order-completed:order-completed-topic}")
    private String orderCompletedTopic;

    @Value("${spring.kafka.topics.return-request-changed:return-request-changed}")
    private String returnRequestChangedTopic;

    public void sendReturnRequestChanged(ReturnRequestChanged event) {
        if (event == null || event.getReturnRequestId() == null) {
            return;
        }
        if (event.getEventId() == null || event.getEventId().isBlank()) {
            event.setEventId(UUID.randomUUID().toString());
        }
        if (event.getChangedAt() == null) {
            event.setChangedAt(Instant.now());
        }

        String messageId = event.getEventId();
        String key = event.getOrderCode() != null ? event.getOrderCode() : String.valueOf(event.getReturnRequestId());
        ProducerRecord<String, Object> record = new ProducerRecord<>(returnRequestChangedTopic, key, event);
        record.headers().add("messageId", messageId.getBytes(StandardCharsets.UTF_8));
        record.headers().add("timestamp", String.valueOf(event.getChangedAt().toEpochMilli()).getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record);
        log.info("Sent return-request-changed: messageId={}, returnId={}, status={} -> topic={}",
            messageId, event.getReturnRequestId(), event.getStatus(), returnRequestChangedTopic);
    }

    public void sendOrderCompleted(OrderCompleted event) {
        if (event == null || event.getOrderCode() == null || event.getOrderCode().isBlank()) {
            return;
        }

        if (event.getEventId() == null || event.getEventId().isBlank()) {
            event.setEventId(UUID.randomUUID().toString());
        }
        if (event.getCompletedAt() == null) {
            event.setCompletedAt(Instant.now());
        }

        String messageId = event.getEventId();
        String key = event.getOrderCode();

        ProducerRecord<String, Object> record = new ProducerRecord<>(orderCompletedTopic, key, event);
        record.headers().add("messageId", messageId.getBytes(StandardCharsets.UTF_8));
        record.headers().add("timestamp", String.valueOf(event.getCompletedAt().toEpochMilli()).getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record);

        log.info("Sent order-completed: messageId={}, orderCode={}, orderId={}, customerKeycloakId={} -> topic={}",
            messageId,
            event.getOrderCode(),
            event.getOrderId(),
            event.getCustomerKeycloakId(),
            orderCompletedTopic
        );
    }

    public void sendSubOrderPayoutRequested(SubOrderPayoutRequested request) {
        if (request == null || request.getSubOrderId() == null) {
            return;
        }

        if (request.getEventId() == null || request.getEventId().isBlank()) {
            request.setEventId(java.util.UUID.randomUUID().toString());
        }
        if (request.getCreatedAt() == null) {
            request.setCreatedAt(Instant.now());
        }

        String messageId = request.getEventId();
        String key = String.valueOf(request.getSubOrderId());

        ProducerRecord<String, Object> record = new ProducerRecord<>(suborderPayoutRequestedTopic, key, request);
        record.headers().add("messageId", messageId.getBytes(StandardCharsets.UTF_8));
        record.headers().add("timestamp", String.valueOf(request.getCreatedAt().toEpochMilli()).getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record);

        log.info("Sent suborder-payout-requested: messageId={}, subOrderId={}, orderCode={}, vendorKeycloakId={} -> topic={}",
            messageId,
            request.getSubOrderId(),
            request.getOrderCode(),
            request.getVendorKeycloakId(),
            suborderPayoutRequestedTopic
        );
    }

    public void sendSubOrderShippingRequested(SubOrderShippingRequested request) {
        if (request == null || request.getSubOrderId() == null) {
            return;
        }

        if (request.getEventId() == null || request.getEventId().isBlank()) {
            request.setEventId(UUID.randomUUID().toString());
        }
        if (request.getCreatedAt() == null) {
            request.setCreatedAt(Instant.now());
        }

        String messageId = request.getEventId();
        String key = String.valueOf(request.getSubOrderId());

        ProducerRecord<String, Object> record = new ProducerRecord<>(suborderShippingRequestedTopic, key, request);
        record.headers().add("messageId", messageId.getBytes(StandardCharsets.UTF_8));
        record.headers().add("timestamp", String.valueOf(request.getCreatedAt().toEpochMilli()).getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record);

        log.info(
            "Sent suborder-shipping-requested: messageId={}, subOrderId={}, orderId={}, carrierPreference={}, serviceLevel={} -> topic={}",
            messageId,
            request.getSubOrderId(),
            request.getOrderId(),
            request.getCarrierPreference(),
            request.getServiceLevel(),
            suborderShippingRequestedTopic
        );
    }

    public void sendStockReservationRequest(StockReservationRequest request) {
        if (request == null) {
            return;
        }

        String messageId = UUID.randomUUID().toString();
        long timestampMs = Instant.now().toEpochMilli();

        String key = request.getOrderId();

        ProducerRecord<String, Object> record = new ProducerRecord<>(
            stockReservationRequestTopic,
            key,
            request
        );
        record.headers().add("messageId", messageId.getBytes(StandardCharsets.UTF_8));
        record.headers().add("timestamp", String.valueOf(timestampMs).getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record);

        log.info("Sent stock-reservation-request: messageId={}, orderId={}, productId={}, quantity={} -> topic={}",
            messageId,
            request.getOrderId(),
            request.getProductId(),
            request.getQuantity(),
            stockReservationRequestTopic
        );
    }

    public void sendStockRollbackRequest(StockRollbackRequest request) {
        if (request == null) {
            return;
        }

        String messageId = UUID.randomUUID().toString();
        long timestampMs = Instant.now().toEpochMilli();

        String key = request.getOrderId();

        ProducerRecord<String, Object> record = new ProducerRecord<>(
            stockRollbackRequestTopic,
            key,
            request
        );
        record.headers().add("messageId", messageId.getBytes(StandardCharsets.UTF_8));
        record.headers().add("timestamp", String.valueOf(timestampMs).getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record);

        log.info("Sent stock-rollback-request: messageId={}, orderId={}, productId={}, quantity={} -> topic={}",
            messageId,
            request.getOrderId(),
            request.getProductId(),
            request.getQuantity(),
            stockRollbackRequestTopic
        );
    }

    public void sendPaymentRefundRequest(RefundRequest request) {
        if (request == null || request.getOrderId() == null || request.getAmount() == null) {
            return;
        }

        String messageId = UUID.randomUUID().toString();
        if (request.getOrderCode() != null && request.getReason() != null && !request.getReason().isBlank()) {
            messageId = request.getOrderCode() + ":" + request.getReason();
        }
        long timestampMs = Instant.now().toEpochMilli();

        String key = String.valueOf(request.getOrderId());
        if (request.getRequestedAt() == null) {
            request.setRequestedAt(Instant.now());
        }
        if (request.getCurrency() == null || request.getCurrency().isBlank()) {
            request.setCurrency("VND");
        }

        ProducerRecord<String, Object> record = new ProducerRecord<>(paymentRefundRequestTopic, key, request);
        record.headers().add("messageId", messageId.getBytes(StandardCharsets.UTF_8));
        record.headers().add("timestamp", String.valueOf(timestampMs).getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record);

        log.info("Sent payment-refund-request: messageId={}, orderId={}, subOrderId={}, amount={} {} -> topic={}",
            messageId,
            request.getOrderId(),
            request.getOrderCode(),
            request.getAmount(),
            request.getCurrency(),
            paymentRefundRequestTopic
        );
    }
}
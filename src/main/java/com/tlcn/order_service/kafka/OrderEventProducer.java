package com.tlcn.order_service.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    public void sendStockReservationRequest(Long orderId, Long productId, int quantity) {
        Map<String, Object> data = new HashMap<>();
        data.put("orderId", orderId);
        data.put("productId", productId);
        data.put("quantity", quantity);
        try {
            String message = objectMapper.writeValueAsString(data);
            kafkaTemplate.send(new ProducerRecord<>("stock-reservation-request", message));
            log.info("Sent stock-reservation-request: orderId={}, productId={}, quantity={}", orderId, productId, quantity);
        } catch (Exception e) {
            log.error("Failed to send stock reservation request: {}", e.getMessage());
            throw new RuntimeException("Failed to send reservation request: " + e.getMessage());
        }
    }

    public void sendStockRollbackRequest(Long productId, int quantity) {
        Map<String, Object> data = new HashMap<>();
        data.put("productId", productId);
        data.put("quantity", quantity);
        try {
            String message = objectMapper.writeValueAsString(data);
            kafkaTemplate.send(new ProducerRecord<>("stock-rollback-request", message));
            log.info("Sent stock-rollback-request: productId={}, quantity={}", productId, quantity);
        } catch (Exception e) {
            log.error("Failed to send stock rollback request: {}", e.getMessage());
            throw new RuntimeException("Failed to send rollback request: " + e.getMessage());
        }
    }
}
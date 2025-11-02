package com.tlcn.order_service.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tlcn.order_service.service.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    @Autowired
    private OrderService orderService;

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "stock-reservation-response", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeStockReservationResponse(String message) {
        try {
            Map<String, Object> data = objectMapper.readValue(message, Map.class);
            Long orderId = ((Number) data.get("orderId")).longValue();
            String status = (String) data.get("status");

            if ("FAILED".equals(status)) {
                log.warn("Stock reservation failed for orderId={}. Rolling back order.", orderId);
                orderService.rollbackOrder(orderId);
            } else {
                log.info("Stock reservation succeeded for orderId={}", orderId);
            }
        } catch (Exception e) {
            log.error("Failed to process stock reservation response: {}", e.getMessage());
        }
    }
}
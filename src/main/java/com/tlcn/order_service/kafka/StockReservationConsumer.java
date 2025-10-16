package com.tlcn.order_service.kafka;

import com.tlcn.order_service.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class StockReservationConsumer {

    private final OrderService orderService;

    // Lắng nghe phản hồi Saga từ Product Service
    @KafkaListener(topics = "stock-reservation-responses", groupId = "order-service-reservation-response")
    public void consumeStockReservationResponse(Map<String, Object> message, Acknowledgment acknowledgment) {
        try {
            log.info("Received stock reservation response: {}", message);

            if (!validateResponseMessage(message)) {
                acknowledgment.acknowledge();
                return;
            }

            Long orderId = ((Number) message.get("orderId")).longValue();
            String status = (String) message.get("status"); // SUCCESS hoặc FAILED
            
            // Xử lý phản hồi Saga
            if ("SUCCESS".equals(status)) {
                // Đặt chỗ thành công -> Xác nhận đơn hàng (giữ nguyên PENDING)
                orderService.confirmOrder(orderId);
                log.info("Order confirmation successful after stock reservation: orderId={}", orderId);
            } else if ("FAILED".equals(status)) {
                // Đặt chỗ thất bại -> Hoàn tác đơn hàng (chuyển sang CANCELLED)
                orderService.compensateOrder(orderId, "Insufficient stock or concurrency issue");
                log.warn("Order cancelled due to stock reservation failure: orderId={}", orderId);
            } else {
                log.warn("Unknown stock reservation status: {}", status);
            }

            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Error processing stock reservation response: {}", e.getMessage(), e);
            // Ném lỗi để DefaultErrorHandler retry
            throw e;
        }
    }
    
    private boolean validateResponseMessage(Map<String, Object> message) {
        if (message == null || !message.containsKey("orderId") || !message.containsKey("status")) {
            log.warn("Invalid reservation response format: {}", message);
            return false;
        }

        Object orderIdObj = message.get("orderId");
        Object statusObj = message.get("status");
        
        if (!(orderIdObj instanceof Number) || !(statusObj instanceof String)) {
            log.warn("Invalid types in reservation response: orderId={}, status={}", orderIdObj, statusObj);
            return false;
        }
        
        String status = (String) statusObj;
        if (!"SUCCESS".equals(status) && !"FAILED".equals(status)) {
            log.warn("Invalid status value in reservation response: {}", status);
            return false;
        }

        return true;
    }
}
package com.tlcn.order_service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tlcn.order_service.dto.OrderRequestDTO;
import com.tlcn.order_service.dto.OrderResponseDTO;
import com.tlcn.order_service.dto.OrderItemDTO;
import com.tlcn.order_service.model.Order;
import com.tlcn.order_service.model.OrderItem;
import com.tlcn.order_service.model.Status;
import com.tlcn.order_service.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderService {
    private final OrderRepository orderRepository;
    private final RestTemplate restTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public static class CustomException extends RuntimeException {
        public CustomException(String message) {
            super(message);
        }
    }

    // =========================================================================
    // SAGA LIFECYCLE HANDLERS (Giữ nguyên logic Saga)
    // =========================================================================

    @Transactional
    public void compensateOrder(Long orderId, String reason) {
        Order order = orderRepository.findById(orderId).orElse(null);

        if (order != null && (order.getStatus() == Status.PENDING || order.getStatus() == Status.PROCESSING)) { 
            order.setStatus(Status.CANCELLED);
            orderRepository.save(order);
            log.warn("Order compensated (cancelled due to Saga failure/stock): orderId={}, reason={}", orderId, reason);
            
            sendOrderStatusUpdate(order.getId(), Status.CANCELLED, order.getCustomerId(), order.getVendorId());
        }
    }

    @Transactional
    public void confirmOrder(Long orderId) {
        Order order = orderRepository.findById(orderId).orElse(null);

        if (order != null && order.getStatus() == Status.PENDING) {
            order.setStatus(Status.PROCESSING);
            orderRepository.save(order);
            
            log.info("Order confirmed (stock reserved) and status updated to PROCESSING: orderId={}", orderId);
            
            sendOrderStatusUpdate(order.getId(), Status.PROCESSING, order.getCustomerId(), order.getVendorId());
        }
    }

    // =========================================================================
    // CRUD & BUSINESS LOGIC
    // =========================================================================

    @Transactional
    public OrderResponseDTO createOrder(OrderRequestDTO request, String keycloakId) {
        // 1. Kiểm tra khách hàng và Quyền hạn
        String customerKeycloakId = getCustomerKeycloakId(request.getCustomerId());
        if (!keycloakId.equals(customerKeycloakId)) {
            throw new CustomException("Unauthorized: Keycloak ID mismatch for customer " + request.getCustomerId());
        }

        // 2. Kiểm tra vendor (Đảm bảo vendor tồn tại)
        try {
            getVendorKeycloakId(request.getVendorId());
        } catch (CustomException e) {
             throw new CustomException("Vendor not found: " + request.getVendorId());
        }

        // 3. Tính toán và kiểm tra tổng tiền (giữ nguyên)
        BigDecimal calculatedTotal = BigDecimal.ZERO;
        for (OrderItemDTO item : request.getItems()) {
            calculatedTotal = calculatedTotal.add(item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())));
        }

        if (request.getTotalAmount().compareTo(calculatedTotal) != 0) {
            throw new CustomException("Total amount mismatch: expected " + calculatedTotal + " but got " + request.getTotalAmount());
        }

        // 4. Tạo và lưu đơn hàng
        final Order order = new Order();
        order.setCustomerId(request.getCustomerId());
        order.setVendorId(request.getVendorId());
        order.setTotalAmount(request.getTotalAmount());
        order.setStatus(Status.PENDING);
        order.setShippingAddress(request.getShippingAddress());

        List<OrderItem> orderItems = request.getItems().stream().map(item -> {
            OrderItem orderItem = new OrderItem();
            orderItem.setOrder(order);
            orderItem.setProductId(item.getProductId());
            orderItem.setQuantity(item.getQuantity());
            orderItem.setPrice(item.getPrice());
            return orderItem;
        }).collect(Collectors.toList());
        order.setItems(orderItems);

        final Order savedOrder = orderRepository.save(order);

        // 5. GỬI SAGA STEP: Yêu cầu Đặt chỗ Tồn kho
        try {
            for (OrderItem item : orderItems) {
                sendStockReservationRequest(savedOrder.getId(), item.getProductId(), item.getQuantity());
            }
        } catch (Exception e) {
            log.error("Failed to send stock reservation request to Kafka, rolling back DB transaction: {}", e.getMessage());
            throw new CustomException("Failed to initiate stock reservation: " + e.getMessage());
        }

        return mapToResponseDTO(savedOrder);
    }

    @Transactional
    public OrderResponseDTO updateOrderStatus(Long orderId, Status newStatus, String keycloakId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new CustomException("Order not found: " + orderId));

        String customerKeycloakId = getCustomerKeycloakId(order.getCustomerId());
        String vendorKeycloakId = getVendorKeycloakId(order.getVendorId());

        boolean isCustomer = keycloakId.equals(customerKeycloakId);
        boolean isVendor = keycloakId.equals(vendorKeycloakId);
        
        if (!isCustomer && !isVendor) {
            throw new CustomException("Unauthorized: Not the customer or vendor of this order");
        }
        
        // KIỂM TRA LOGIC CHUYỂN TRẠNG THÁI & HỦY ĐƠN HÀNG (CANCELLED)
        
        if (newStatus == Status.CANCELLED) {
            if (order.getStatus() == Status.DELIVERED || order.getStatus() == Status.CANCELLED) {
                throw new CustomException("Order cannot be cancelled in status: " + order.getStatus());
            }
            // Gửi sự kiện hủy (SAGA Compensation)
            order.setStatus(newStatus);
            orderRepository.save(order);
            sendOrderStatusUpdate(order.getId(), newStatus, order.getCustomerId(), order.getVendorId());
            return mapToResponseDTO(order);
            
        } else if (isCustomer) {
            // Customer chỉ được phép HỦY (CANCELLED) hoặc xem.
             throw new CustomException("Customer can only update status to CANCELLED.");
        } 
        
        // Logic chuyển trạng thái cho Vendor (PROCESSING, SHIPPING, DELIVERED)
        if (isVendor) {
            if (order.getStatus() == Status.CANCELLED) {
                throw new CustomException("Cannot change status for a CANCELLED order.");
            }
            if (newStatus == Status.PENDING) {
                throw new CustomException("Vendor cannot manually set status to PENDING.");
            }
             // Cần thêm logic kiểm tra luồng trạng thái hợp lệ ở đây (ví dụ: không thể SHIPPING nếu chưa PROCESSING)
        }


        order.setStatus(newStatus);
        orderRepository.save(order);

        sendOrderStatusUpdate(order.getId(), newStatus, order.getCustomerId(), order.getVendorId());

        return mapToResponseDTO(order);
    }

    public OrderResponseDTO getOrderDetails(Long orderId, String keycloakId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new CustomException("Order not found: " + orderId));

        String customerKeycloakId = getCustomerKeycloakId(order.getCustomerId());
        String vendorKeycloakId = getVendorKeycloakId(order.getVendorId());
        
        if (!keycloakId.equals(customerKeycloakId) && !keycloakId.equals(vendorKeycloakId)) {
            throw new CustomException("Unauthorized: You do not have permission to view this order.");
        }
        
        return mapToResponseDTO(order);
    }

    public List<OrderResponseDTO> getOrdersByCustomerKeycloakId(String keycloakId) {
        Long customerId = getCustomerIdFromKeycloakId(keycloakId);
        return orderRepository.findByCustomerId(customerId).stream()
                .map(this::mapToResponseDTO)
                .collect(Collectors.toList());
    }

    public List<OrderResponseDTO> getOrdersByVendorKeycloakId(String keycloakId) {
        Long vendorId = getVendorIdFromKeycloakId(keycloakId);
        return orderRepository.findByVendorId(vendorId).stream()
                .map(this::mapToResponseDTO)
                .collect(Collectors.toList());
    }
    
    // =========================================================================
    // UTILITY METHODS (Giao tiếp với các Service khác)
    // =========================================================================
    
    // Lấy Keycloak ID của Customer từ Customer Service (Dùng ID nội bộ)
    private String getCustomerKeycloakId(Long customerId) {
        String customerServiceUrl = "http://customer-service/api/customers/" + customerId;
        try {
            Map<String, Object> response = restTemplate.getForObject(customerServiceUrl, Map.class);
            if (response == null || response.get("data") == null) {
                throw new CustomException("Customer data not found: " + customerId);
            }
            Map<String, Object> customer = (Map<String, Object>) response.get("data");
            String keycloakId = (String) customer.get("keycloakId");
            if (keycloakId == null) throw new CustomException("Customer Keycloak ID is missing.");
            return keycloakId;
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                throw new CustomException("Customer not found: " + customerId);
            }
            throw new CustomException("Error calling Customer Service: " + e.getMessage());
        }
    }
    
    // Lấy Keycloak ID của Vendor từ Vendor Service (Dùng ID nội bộ)
    private String getVendorKeycloakId(Long vendorId) {
        String vendorServiceUrl = "http://vendor-service/api/vendors/" + vendorId;
        try {
            Map<String, Object> response = restTemplate.getForObject(vendorServiceUrl, Map.class);
            if (response == null || response.get("data") == null) {
                throw new CustomException("Vendor data not found: " + vendorId);
            }
            Map<String, Object> vendor = (Map<String, Object>) response.get("data");
            String keycloakId = (String) vendor.get("keycloakId");
            if (keycloakId == null) throw new CustomException("Vendor Keycloak ID is missing.");
            return keycloakId;
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                throw new CustomException("Vendor not found: " + vendorId);
            }
            throw new CustomException("Error calling Vendor Service: " + e.getMessage());
        }
    }
    
    public Long getCustomerIdFromKeycloakId(String keycloakId) {
        String customerServiceUrl = "http://customer-service/api/customers/find-id-by-keycloak?keycloakId=" + keycloakId;
        
        try {
            // Sử dụng ParameterizedTypeReference để xử lý ResponseDTO<Long>
            ResponseEntity<Map<String, Object>> responseEntity = restTemplate.exchange(
                customerServiceUrl, 
                HttpMethod.GET, 
                null, 
                new ParameterizedTypeReference<Map<String, Object>>() {}
            );

            Map<String, Object> responseBody = responseEntity.getBody();
            if (responseBody == null || responseBody.get("data") == null) {
                throw new CustomException("Customer not found for Keycloak ID: " + keycloakId);
            }
            
            Object idObject = responseBody.get("data");
            if (idObject instanceof Integer) {
                return ((Integer) idObject).longValue();
            } else if (idObject instanceof Long) {
                return (Long) idObject;
            } else {
                 throw new CustomException("Customer ID format error from Customer Service.");
            }
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                throw new CustomException("Customer not found for Keycloak ID: " + keycloakId);
            }
            throw new CustomException("Error calling Customer Service: " + e.getMessage());
        }
    }

    public Long getVendorIdFromKeycloakId(String keycloakId) {
        String vendorServiceUrl = "http://vendor-service/api/vendors/find-id-by-keycloak?keycloakId=" + keycloakId;
        
        try {
            // Sử dụng ParameterizedTypeReference để xử lý ResponseDTO<Long>
            ResponseEntity<Map<String, Object>> responseEntity = restTemplate.exchange(
                vendorServiceUrl, 
                HttpMethod.GET, 
                null, 
                new ParameterizedTypeReference<Map<String, Object>>() {}
            );

            Map<String, Object> responseBody = responseEntity.getBody();
            if (responseBody == null || responseBody.get("data") == null) {
                throw new CustomException("Vendor not found for Keycloak ID: " + keycloakId);
            }
            
            Object idObject = responseBody.get("data");
            if (idObject instanceof Integer) {
                return ((Integer) idObject).longValue();
            } else if (idObject instanceof Long) {
                return (Long) idObject;
            } else {
                 throw new CustomException("Vendor ID format error from Vendor Service.");
            }
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                throw new CustomException("Vendor not found for Keycloak ID: " + keycloakId);
            }
            throw new CustomException("Error calling Vendor Service: " + e.getMessage());
        }
    }

    private void sendStockReservationRequest(Long orderId, Long productId, int quantity) {
        Map<String, Object> request = new HashMap<>();
        request.put("orderId", orderId);
        request.put("productId", productId);
        request.put("quantity", quantity);
        request.put("source", "order-service");
        
        kafkaTemplate.send("stock-reservation-requests", String.valueOf(orderId), request); 
        log.info("Sent stock reservation request: orderId={}, productId={}, quantity={}", orderId, productId, quantity);
    }
    
    private void sendOrderStatusUpdate(Long orderId, Status status, Long customerId, Long vendorId) {
        Map<String, Object> event = new HashMap<>();
        event.put("orderId", orderId);
        event.put("status", status.name());
        event.put("customerId", customerId);
        event.put("vendorId", vendorId);
        event.put("source", "order-service");
        try {
            kafkaTemplate.send("order-status-updates", String.valueOf(orderId), event);
            log.info("Sent order status update to Kafka: orderId={}, status={}", orderId, status);
        } catch (Exception e) {
            throw new CustomException("Failed to send order status update to Kafka: " + e.getMessage());
        }
    }

    private OrderResponseDTO mapToResponseDTO(Order order) {
        OrderResponseDTO response = new OrderResponseDTO();
        response.setId(order.getId());
        response.setCustomerId(order.getCustomerId());
        response.setVendorId(order.getVendorId());
        response.setTotalAmount(order.getTotalAmount());
        response.setStatus(order.getStatus());
        response.setShippingAddress(order.getShippingAddress());
        response.setCreatedAt(order.getCreatedAt());
        response.setUpdatedAt(order.getUpdatedAt());
        response.setItems(order.getItems().stream().map(item -> {
            OrderItemDTO itemDTO = new OrderItemDTO();
            itemDTO.setProductId(item.getProductId());
            itemDTO.setQuantity(item.getQuantity());
            itemDTO.setPrice(item.getPrice());
            return itemDTO;
        }).collect(Collectors.toList()));
        return response;
    }
}
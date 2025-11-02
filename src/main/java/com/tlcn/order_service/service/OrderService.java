package com.tlcn.order_service.service;

import com.tlcn.order_service.dto.OrderDTO;
import com.tlcn.order_service.dto.ProductDTO;
import com.tlcn.order_service.kafka.OrderEventProducer; 
import com.tlcn.order_service.model.*;
import com.tlcn.order_service.repository.CartRepository;
import com.tlcn.order_service.repository.OrderItemRepository;
import com.tlcn.order_service.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.ArrayList;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    @Autowired private OrderRepository orderRepository;
    @Autowired private OrderItemRepository orderItemRepository;
    @Autowired private CartRepository cartRepository;
    @Autowired private RestTemplate restTemplate;
    @Autowired private OrderEventProducer eventProducer; 
    private static final String PAYPROMO_SERVICE_URL = "http://paypromo-service/api/discounts";

    public static class CustomException extends RuntimeException {
        public CustomException(String message) { super(message); }
    }

    /** ---------- TẠO ĐƠN HÀNG ---------- **/
    @Transactional
    public Order createOrder(OrderDTO orderDTO, String keycloakId) {
        log.info("Creating order for customer: keycloakId={}", keycloakId);
        Cart cart = cartRepository.findByKeycloakId(keycloakId)
                .orElseThrow(() -> new CustomException("Cart not found for user: " + keycloakId));

        if (cart.getCartItems().isEmpty()) {
            throw new CustomException("Cart is empty");
        }

        // 1. TẠO VÀ LƯU ORDER ĐỂ CÓ ID HỢP LỆ CHO SAGA
        Order order = new Order();
        order.setKeycloakId(keycloakId);
        order.setShippingAddress(orderDTO.getShippingAddress());
        order.setTotalPrice(cart.getTotalPrice());
        Order savedOrder = orderRepository.save(order); // LƯU LẦN 1

        // 2. Duyệt CartItems, kiểm tra, gửi Kafka và tạo OrderItem
        Map<String, List<CartItem>> itemsByVendor = cart.getCartItems().stream()
                .collect(Collectors.groupingBy(CartItem::getVendorKeycloakId));

        for (Map.Entry<String, List<CartItem>> e : itemsByVendor.entrySet()) {
            String vendorKeycloakId = e.getKey();
            List<CartItem> vendorItems = e.getValue();
            
            for (CartItem ci : vendorItems) {
                // Kiểm tra tồn kho (Blocking call)
                ProductDTO product = restTemplate.getForObject(
                        "http://product-service/api/products/" + ci.getProductId(),
                        ProductDTO.class);
                if (product == null || product.getStock() < ci.getQuantity()) {
                    throw new CustomException("Insufficient stock for product ID: " + ci.getProductId());
                }

                // Dự trữ kho (Kafka - Saga Request)
                // Gửi ID hợp lệ của savedOrder
                eventProducer.sendStockReservationRequest(
                        savedOrder.getId(), ci.getProductId(), ci.getQuantity());

                // Tạo OrderItem
                OrderItem oi = new OrderItem();
                oi.setOrder(savedOrder); 
                oi.setProductId(ci.getProductId());
                oi.setVendorKeycloakId(vendorKeycloakId);
                oi.setQuantity(ci.getQuantity());
                oi.setUnitPrice(ci.getPrice() / ci.getQuantity());
                oi.setStatus(Status.PENDING);
                savedOrder.getOrderItems().add(oi);
            }
        }

        // 3. Xóa giỏ hàng
        cart.getCartItems().clear();
        cart.setTotalPrice(0.0);
        cartRepository.save(cart);

        // LƯU LẦN 2 để cập nhật OrderItems 
        Order finalOrder = orderRepository.save(savedOrder);
        log.info("Order created: id={}, keycloakId={}", finalOrder.getId(), keycloakId);
        return finalOrder;
    }

    /** ---------- LẤY ĐƠN HÀNG KHÁCH HÀNG ---------- **/
    public Order getOrder(Long id, String keycloakId) {
        Order order = orderRepository.findById(id)
                .orElseThrow(() -> new CustomException("Order not found: " + id));
        if (!order.getKeycloakId().equals(keycloakId)) {
            throw new CustomException("Unauthorized");
        }
        return order;
    }

    public Page<Order> getOrdersByCustomer(String keycloakId, Pageable pageable) {
        return orderRepository.findByKeycloakId(keycloakId, pageable);
    }

    /** ---------- HỦY ĐƠN HÀNG KHÁCH HÀNG ---------- **/
    @Transactional
    public void cancelOrder(Long id, String keycloakId) {
        Order order = getOrder(id, keycloakId);
        for (OrderItem oi : order.getOrderItems()) {
            if (oi.getStatus() != Status.PENDING) {
                throw new CustomException("Cannot cancel item with status: " + oi.getStatus());
            }
            oi.setStatus(Status.CANCELLED);
            eventProducer.sendStockRollbackRequest(oi.getProductId(), oi.getQuantity());
        }
        orderRepository.save(order);
        log.info("Order cancelled: id={}", id);
    }

    /** ---------- VENDOR: LẤY DANH SÁCH ORDER ITEM ---------- **/
    public Page<OrderItem> getOrdersByVendor(String vendorKeycloakId, Pageable pageable) {
        return orderItemRepository.findByVendorKeycloakId(vendorKeycloakId, pageable);
    }

    public OrderItem getVendorOrderItem(Long itemId, String vendorKeycloakId) {
        OrderItem oi = orderItemRepository.findById(itemId)
                .orElseThrow(() -> new CustomException("Item not found: " + itemId));
        if (!oi.getVendorKeycloakId().equals(vendorKeycloakId)) {
            throw new CustomException("Unauthorized");
        }
        return oi;
    }

    /** ---------- VENDOR: CẬP NHẬT TRẠNG THÁI ITEM ---------- **/
    @Transactional
    public void updateItemStatus(Long itemId, String vendorKeycloakId, String statusStr) {
        OrderItem oi = getVendorOrderItem(itemId, vendorKeycloakId);
        try {
            Status newStatus = Status.valueOf(statusStr.toUpperCase());
            if (!isValidTransition(oi.getStatus(), newStatus)) {
                throw new CustomException("Invalid transition " + oi.getStatus() + " → " + newStatus);
            }
            oi.setStatus(newStatus);
            orderItemRepository.save(oi);
            log.info("Item status updated: itemId={}, newStatus={}", itemId, newStatus);
        } catch (IllegalArgumentException e) {
             throw new CustomException("Invalid status value: " + statusStr);
        }
    }

    /** ---------- VENDOR: HỦY ITEM ---------- **/
    @Transactional
    public void cancelVendorItem(Long itemId, String vendorKeycloakId) {
        OrderItem oi = getVendorOrderItem(itemId, vendorKeycloakId);
        if (oi.getStatus() != Status.PENDING) {
            throw new CustomException("Cannot cancel item with status: " + oi.getStatus());
        }
        oi.setStatus(Status.CANCELLED);
        eventProducer.sendStockRollbackRequest(oi.getProductId(), oi.getQuantity());
        orderItemRepository.save(oi);
        log.info("Vendor item cancelled: itemId={}", itemId);
    }

    /** ---------- ROLLBACK KHI THANH TOÁN THẤT BẠI (Từ Consumer) ---------- **/
    @Transactional
    public void rollbackOrder(Long orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new CustomException("Order not found: " + orderId));
        for (OrderItem oi : order.getOrderItems()) {
            if (oi.getStatus() == Status.PENDING) {
                oi.setStatus(Status.CANCELLED);
                eventProducer.sendStockRollbackRequest(oi.getProductId(), oi.getQuantity());
            }
        }
        orderRepository.save(order);
        log.info("Order rolled back: id={}", orderId);
    }

    /* ------------------- HỖ TRỢ ------------------- */

    private boolean isValidTransition(Status current, Status next) {
        return switch (current) {
            case PENDING -> next == Status.PROCESSING || next == Status.CANCELLED;
            case PROCESSING -> next == Status.SHIPPED;
            case SHIPPED -> next == Status.DELIVERED;
            default -> false; 
        };
    }
}